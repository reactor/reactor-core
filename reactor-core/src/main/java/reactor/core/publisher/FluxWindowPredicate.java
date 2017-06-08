/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxBufferPredicate.Mode;
import javax.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;

/**
 * Cut a sequence into non-overlapping windows where each window boundary is determined by
 * a {@link Predicate} on the values. The predicate can be used in several modes:
 * <ul>
 * <li>{@code Until}: A new window starts when the predicate returns true. The
 * element that just matched the predicate is the last in the previous window.</li>
 * <li>{@code UntilOther}: A new window starts when the predicate returns true. The
 * element that just matched the predicate is the first in the new window.</li>
 * <li>{@code While}: A new window starts when the predicate stops matching. The
 * non-matching elements that delimit each window are simply discarded, and the
 * windows are not emitted before an inner element is pushed</li>
 * </ul>
 *
 * @param <T> the source and window value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowPredicate<T> extends FluxOperator<T, GroupedFlux<T, T>>
		implements Fuseable{

	final Supplier<? extends Queue<T>> groupQueueSupplier;

	final Supplier<? extends Queue<GroupedFlux<T, T>>> mainQueueSupplier;

	final Mode mode;

	final Predicate<? super T> predicate;

	final int prefetch;

	FluxWindowPredicate(Flux<? extends T> source,
			Supplier<? extends Queue<GroupedFlux<T, T>>> mainQueueSupplier,
			Supplier<? extends Queue<T>> groupQueueSupplier,
			int prefetch,
			Predicate<? super T> predicate,
			Mode mode) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.predicate = Objects.requireNonNull(predicate, "predicate");
		this.mainQueueSupplier =
				Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.groupQueueSupplier =
				Objects.requireNonNull(groupQueueSupplier, "groupQueueSupplier");
		this.mode = mode;
		this.prefetch = prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super GroupedFlux<T, T>> s, Context ctx) {
		source.subscribe(new WindowPredicateMain<>(s,
				mainQueueSupplier.get(),
				groupQueueSupplier,
				prefetch,
				predicate,
				mode, ctx), ctx);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	static final class WindowPredicateMain<T>
			implements Fuseable.QueueSubscription<GroupedFlux<T, T>>,
			           InnerOperator<T, GroupedFlux<T, T>> {

		final Subscriber<? super GroupedFlux<T, T>> actual;

		final Supplier<? extends Queue<T>> groupQueueSupplier;

		final Mode mode;

		final Predicate<? super T> predicate;

		final int prefetch;

		final Queue<GroupedFlux<T, T>> queue;

		WindowGroupedFlux<T> window;

		Context context;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateMain.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowPredicateMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowPredicateMain.class, "requested");

		volatile boolean   done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowPredicateMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(WindowPredicateMain.class,
						Throwable.class,
						"error");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateMain> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateMain.class,
						"cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateMain> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateMain.class, "windowCount");

		Subscription s;

		volatile boolean outputFused;

		WindowPredicateMain(Subscriber<? super GroupedFlux<T, T>> actual,
				Queue<GroupedFlux<T, T>> queue,
				Supplier<? extends Queue<T>> groupQueueSupplier,
				int prefetch,
				Predicate<? super T> predicate,
				Mode mode,
				Context ctx) {
			this.actual = actual;
			this.queue = queue;
			this.groupQueueSupplier = groupQueueSupplier;
			this.prefetch = prefetch;
			this.predicate = predicate;
			this.mode = mode;
			WINDOW_COUNT.lazySet(this, 2);
			this.context = ctx;
			initializeWindow();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				if (cancelled == 0) {
					if (prefetch == Integer.MAX_VALUE) {
						s.request(Long.MAX_VALUE);
					}
					else {
						s.request(prefetch);
					}
				}
			}
		}

		void initializeWindow() {
			WindowGroupedFlux<T> g = new WindowGroupedFlux<>(null,
					groupQueueSupplier.get(),
					this,
					context);
			window = g;
			queue.offer(g);
		}

		void offerNewWindow(T key, @Nullable T emitInNewWindow) {
			// if the main is cancelled, don't create new groups
			if (cancelled == 0) {
				WINDOW_COUNT.getAndIncrement(this);

				WindowGroupedFlux<T> g = new WindowGroupedFlux<>(key,
						groupQueueSupplier.get(), this, context);
				if (emitInNewWindow != null) {
					g.onNext(emitInNewWindow);
				}
				window = g;

				if (!queue.offer(g)) {
					onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), emitInNewWindow));
					return;
				}
				drain();
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			WindowGroupedFlux<T> g = window;

			boolean match;
			try {
				match = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			if (mode == Mode.UNTIL && match) {
				g.onNext(t);
				g.onComplete();
				offerNewWindow(t, null);
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				g.onComplete();
				offerNewWindow(t, t);
			}
			else if (mode == Mode.WHILE && !match) {
				g.onComplete();
				offerNewWindow(t, null);
				//compensate for the dropped delimiter
				s.request(1);
			}
			else {
				g.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if(done){
				return;
			}

			WindowGroupedFlux<T> g = window;
			if (g != null) {
				g.onComplete();
			}
			window = null;
			done = true;
			WINDOW_COUNT.decrementAndGet(this);
			drain();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled == 1;
			if (key == IntAttr.PREFETCH) return prefetch;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.BUFFERED) return queue.size();
			if (key == ThrowableAttr.ERROR) return error;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}

		@Override
		public Subscriber<? super GroupedFlux<T, T>> actual() {
			return actual;
		}

		void signalAsyncError() {
			Throwable e = Exceptions.terminate(ERROR, this);
			windowCount = 0;
			WindowGroupedFlux<T> g = window;
			if (g != null) {
				g.onError(e);
			}
			actual.onError(e);
			window = null;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				if (WINDOW_COUNT.decrementAndGet(this) == 0) {
					s.cancel();
				}
				else if (!outputFused) {
					if (WIP.getAndIncrement(this) == 0) {
						// remove queued up but unobservable groups from the mapping
						GroupedFlux<T, T> g;
						while ((g = queue.poll()) != null) {
							((WindowGroupedFlux<T>) g).cancel();
						}

						if (WIP.decrementAndGet(this) == 0) {
							return;
						}

						drainLoop();
					}
				}
			}
		}

		void groupTerminated() {
			if (windowCount == 0) {
				return;
			}
			window = null;
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (outputFused) {
				drainFused();
			}
			else {
				drainLoop();
			}
		}

		void drainFused() {
			int missed = 1;

			final Subscriber<? super GroupedFlux<T, T>> a = actual;
			final Queue<GroupedFlux<T, T>> q = queue;

			for (; ; ) {

				if (cancelled != 0) {
					q.clear();
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						signalAsyncError();
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainLoop() {

			int missed = 1;

			Subscriber<? super GroupedFlux<T, T>> a = actual;
			Queue<GroupedFlux<T, T>> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (e != r) {
					boolean d = done;
					GroupedFlux<T, T> v = q.poll();
					boolean empty = v == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0L) {

					s.request(e);

					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d,
				boolean empty,
				Subscriber<?> a,
				Queue<GroupedFlux<T, T>> q) {

			if (cancelled != 0) {
				q.clear();
				return true;
			}
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					queue.clear();
					signalAsyncError();
					return true;
				}
				else if (empty) {
					a.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
		@Nullable
		public GroupedFlux<T, T> poll() {
			return queue.poll();
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}
	}

	static final class WindowGroupedFlux<T> extends GroupedFlux<T, T>
			implements Fuseable, Fuseable.QueueSubscription<T>, InnerOperator<T, T> {

		final T key;

		@Override
		@Nullable
		public T key() {
			return key;
		}

		final Queue<T> queue;

		final Context context;

		volatile WindowPredicateMain<T> parent;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowGroupedFlux, WindowPredicateMain>
				PARENT = AtomicReferenceFieldUpdater.newUpdater(WindowGroupedFlux.class,
				WindowPredicateMain.class,
				"parent");

		volatile boolean done;
		Throwable error;

		volatile Subscriber<? super T> actual;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowGroupedFlux, Subscriber> ACTUAL =
				AtomicReferenceFieldUpdater.newUpdater(WindowGroupedFlux.class,
						Subscriber.class,
						"actual");

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowGroupedFlux> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(WindowGroupedFlux.class, "once");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowGroupedFlux> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowGroupedFlux.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowGroupedFlux> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowGroupedFlux.class, "requested");

		volatile boolean enableOperatorFusion;

		int produced;

		WindowGroupedFlux(
				@Nullable T key,
				Queue<T> queue,
				WindowPredicateMain<T> parent,
				Context ctx) {
			this.key = key;
			this.context = ctx;
			this.queue = queue;
			this.parent = parent;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		void propagateTerminate() {
			WindowPredicateMain<T> r = parent;
			if (r != null && PARENT.compareAndSet(this, r, null)) {
				r.groupTerminated();
			}
		}

		void propagateCancel() {
			WindowPredicateMain<T> r = parent;
			if (r != null && PARENT.compareAndSet(this, r, null)) {
				r.cancel();
			}
		}

		void drainRegular(Subscriber<? super T> a) {
			int missed = 1;

			final Queue<T> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					T t = q.poll();
					boolean empty = t == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (r == e) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0) {
					WindowPredicateMain<T> main = parent;
					if (main != null) {
						main.s.request(e);
					}
					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainFused(Subscriber<? super T> a) {
			int missed = 1;

			final Queue<T> q = queue;

			for (; ; ) {

				if (cancelled) {
					q.clear();
					actual = null;
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					actual = null;

					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain() {
			Subscriber<? super T> a = actual;
			if (a != null) {
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}

				if (enableOperatorFusion) {
					drainFused(a);
				}
				else {
					drainRegular(a);
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				q.clear();
				actual = null;
				return true;
			}
			if (d && empty) {
				Throwable e = error;
				actual = null;
				if (e != null) {
					a.onError(e);
				}
				else {
					a.onComplete();
				}
				return true;
			}

			return false;
		}

		public void onNext(T t) {
			Subscriber<? super T> a = actual;

			if (!queue.offer(t)) {
				onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t));
				return;
			}
			if (enableOperatorFusion) {
				if (a != null) {
					a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
				}
			}
			else {
				drain();
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			//IGNORE
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;

			propagateTerminate();

			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			propagateTerminate();

			drain();
		}

		@Override
		public void subscribe(Subscriber<? super T> s, Context ctx) {
			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
				ContextRelay.set(s, context);
				s.onSubscribe(this);
				ACTUAL.lazySet(this, s);
				drain();
			}
			else {
				s.onError(new IllegalStateException(
						"This processor allows only a single Subscriber"));
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			propagateCancel();

			if (!enableOperatorFusion) {
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null) {
				produced++;
			}
			else {
				int p = produced;
				if (p != 0) {
					produced = 0;
					WindowPredicateMain<T> main = parent;
					if (main != null) {
						main.s.request(p);
					}
				}
			}
			return v;
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				enableOperatorFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return parent;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.BUFFERED) return queue == null ? 0 : queue.size();
			if (key == ThrowableAttr.ERROR) return error;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return "WindowGroupedFlux[" + key + "]";
		}
	}

}

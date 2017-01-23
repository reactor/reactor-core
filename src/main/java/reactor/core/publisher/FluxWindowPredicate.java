/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.FluxBufferPredicate.Mode;

/**
 * Cut a sequence into non-overlapping windows where each window boundary is determined by
 * a {@link Predicate} on the values. The predicate can be used in several modes:
 * <ul>
 *     <li>{@code Until}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the last in the previous window.</li>
 *     <li>{@code UntilOther}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the first in the new window.</li>
 *     <li>{@code While}: A new window starts when the predicate stops matching. The
 *     non-matching elements that delimit each window are simply discarded, and the
 *     windows are not emitted before an inner element is pushed</li>
 * </ul>
 *
 * @param <T> the source and window value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowPredicate<T>
		extends FluxSource<T, GroupedFlux<T, T>> {

	final Supplier<? extends Queue<T>> groupQueueSupplier;
	
	final Supplier<? extends Queue<GroupedFlux<T, T>>> mainQueueSupplier;
	
	final Mode mode;
	
	final Predicate<? super T> predicate;

	final int prefetch;

	public FluxWindowPredicate(
			Publisher<? extends T> source,
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
		this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.groupQueueSupplier = Objects.requireNonNull(groupQueueSupplier, "groupQueueSupplier");
		this.mode = mode;
		this.prefetch = prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super GroupedFlux<T, T>> s) {
		Queue<GroupedFlux<T, T>> q;

		try {
			q = mainQueueSupplier.get();
		} catch (Throwable ex) {
			Operators.error(s, Operators.onOperatorError(ex));
			return;
		}

		if (q == null) {
			Operators.error(s, new NullPointerException("The mainQueueSupplier returned a null queue"));
			return;
		}

		source.subscribe(new WindowPredicateMain<>(s, q, groupQueueSupplier, prefetch, predicate, mode));
	}

	@Override
	public long getPrefetch() {
		return prefetch;
	}

	static final class WindowPredicateMain<T> implements Subscriber<T>,
	                                                   Fuseable.QueueSubscription<GroupedFlux<T, T>>,
	                                                   MultiProducer, Producer, Trackable,
	                                                   Receiver {

		final Subscriber<? super GroupedFlux<T, T>> actual;

		final Supplier<? extends Queue<T>> groupQueueSupplier;

		final Mode mode;

		final Predicate<? super T> predicate;

		final int prefetch;

		final Queue<GroupedFlux<T, T>> queue;

		WindowGroupedFlux<T> window;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateMain.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowPredicateMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowPredicateMain.class, "requested");

		volatile boolean done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowPredicateMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(WindowPredicateMain.class, Throwable.class, "error");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateMain> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateMain.class, "cancelled");

		Subscription s;

		volatile boolean enableAsyncFusion;

		public WindowPredicateMain(
				Subscriber<? super GroupedFlux<T, T>> actual,
				Queue<GroupedFlux<T, T>> queue,
				Supplier<? extends Queue<T>> groupQueueSupplier,
				int prefetch,
				Predicate<? super T> predicate,
				Mode mode) {
			this.actual = actual;
			this.queue = queue;
			this.groupQueueSupplier = groupQueueSupplier;
			this.prefetch = prefetch;
			this.predicate = predicate;
			this.mode = mode;
			initializeWindow();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				if (error != null) {
					s.cancel();
					done = true;
					drain();
				} else {
					s.request(prefetch);
				}
			}
		}

		public void initializeWindow() {
			T key = null;
			Queue<T> q;

			try {
				q = groupQueueSupplier.get();
			} catch (Throwable ex) {
				ERROR.compareAndSet(this, null, ex);
				return;
			}

			if (q == null) {
				ERROR.compareAndSet(this, null, new NullPointerException("The groupQueueSupplier returned a null queue"));
				return;
			}

			WindowGroupedFlux<T> g = new WindowGroupedFlux<>(key, q, this, prefetch);
			window = g;
			queue.offer(g);
		}

		public boolean offerNewWindow(T key, T emitInNewWindow) {
			// if the main is cancelled, don't create new groups
			if (cancelled == 0) {
				Queue<T> q;

				try {
					q = groupQueueSupplier.get();
				} catch (Throwable ex) {
					onError(Operators.onOperatorError(s, ex, key));
					return false;
				}

				if (q == null) {
					onError(Operators.onOperatorError(s, new NullPointerException("The mainQueueSupplier returned a null queue"), key));
					return false;
				}

				WindowGroupedFlux<T> g = new WindowGroupedFlux<>(key, q, this, prefetch);
				if (emitInNewWindow != null) {
					g.onNext(emitInNewWindow);
				}
				window = g;

				queue.offer(g);
				drain();
			}
			return true;
		}

		@Override
		public void onNext(T t) {
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
				offerNewWindow(t,null);
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				g.onComplete();
				offerNewWindow(t, t);
			}
			else if (mode == Mode.WHILE && !match) {
				g.onComplete();
				offerNewWindow(t, null);
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
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			WindowGroupedFlux<T> g = window;
			if (g != null) {
				g.onComplete();
			}
			window = null;
			done = true;
			drain();
		}

		@Override
		public long getCapacity() {
			return prefetch;
		}

		@Override
		public long getPending() {
			return queue.size();
		}

		@Override
		public boolean isStarted() {
			return s != null && cancelled != 1 && !done;
		}

		@Override
		public boolean isCancelled() {
			return cancelled == 1;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Iterator<?> downstreams() {
			WindowGroupedFlux<T> g = window;
			if (g == null)
				return Collections.emptyList().iterator();
			return Collections.singletonList(g).iterator();
		}

		@Override
		public long downstreamCount() {
			return window == null ? 0L : 1L;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		void signalAsyncError() {
			Throwable e = Exceptions.terminate(ERROR, this);
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
					s.cancel();
			}
		}

		void groupTerminated(T key) {
			WindowGroupedFlux<T> g = window;
			//TODO check logic
			if (g != null && Objects.equals(key, g.key())) { //key can be null
				window = null;
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (enableAsyncFusion) {
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

			for (;;) {

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

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<GroupedFlux<T, T>> q) {
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					queue.clear();
					signalAsyncError();
					return true;
				} else
				if (empty) {
					a.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
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
			if (requestedMode == Fuseable.ANY || requestedMode == Fuseable.ASYNC) {
				enableAsyncFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		void requestInner(long n) {
			s.request(n);
		}
	}

	static final class WindowGroupedFlux<T> extends GroupedFlux<T, T>
			implements Fuseable, Fuseable.QueueSubscription<T>, Producer, Receiver, Trackable {
		final T key;

		final int limit;

		@Override
		public T key() {
			return key;
		}

		final Queue<T> queue;

		volatile WindowPredicateMain<T> parent;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowGroupedFlux, WindowPredicateMain> PARENT =
				AtomicReferenceFieldUpdater.newUpdater(WindowGroupedFlux.class, WindowPredicateMain.class, "parent");

		volatile boolean done;
		Throwable error;

		volatile Subscriber<? super T> actual;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowGroupedFlux, Subscriber> ACTUAL =
				AtomicReferenceFieldUpdater.newUpdater(WindowGroupedFlux.class, Subscriber.class, "actual");

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

		public WindowGroupedFlux(T key, Queue<T> queue, WindowPredicateMain<T> parent, int prefetch) {
			this.key = key;
			this.queue = queue;
			this.parent = parent;
			this.limit = prefetch - (prefetch >> 2);
		}

		void doTerminate() {
			WindowPredicateMain<T> r = parent;
			if (r != null && PARENT.compareAndSet(this, r, null)) {
				r.groupTerminated(key);
			}
		}

		void drainRegular(Subscriber<? super T> a) {
			int missed = 1;

			final Queue<T> q = queue;

			for (;;) {

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
						main.requestInner(e);
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

			for (;;) {

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
					} else {
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
				} else {
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
				} else {
					a.onComplete();
				}
				return true;
			}

			return false;
		}

		public void onNext(T t) {
			if (done || cancelled) {
				return;
			}

			Subscriber<? super T> a = actual;

			if (!queue.offer(t)) {
				onError(Exceptions.failWithOverflow("The queue is full"));
				return;
			}
			if (enableOperatorFusion) {
				if (a != null) {
					a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
				}
			} else {
				drain();
			}
		}

		public void onError(Throwable t) {
			if (done || cancelled) {
				return;
			}

			error = t;
			done = true;

			doTerminate();

			drain();
		}

		public void onComplete() {
			if (done || cancelled) {
				return;
			}

			done = true;

			doTerminate();

			drain();
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

				s.onSubscribe(this);
				actual = s;
				if (cancelled) {
					actual = null;
				} else {
					drain();
				}
			} else {
				s.onError(new IllegalStateException("This processor allows only a single Subscriber"));
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

			doTerminate();

			if (!enableOperatorFusion) {
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}

		@Override
		public T poll() {
			T v = queue.poll();
			if (v != null) {
				produced++;
			} else {
				int p = produced;
				if (p != 0) {
					produced = 0;
					WindowPredicateMain<T> main = parent;
					if (main != null) {
						main.requestInner(p);
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
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return once == 1 && !done && !cancelled;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return parent;
		}

		@Override
		public long getCapacity() {
			WindowPredicateMain<T> parent = this.parent;
			return parent != null ? parent.prefetch : -1L;
		}

		@Override
		public long getPending() {
			return queue == null || done ? -1L : queue.size();
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public long expectedFromUpstream() {
			return produced;
		}

		@Override
		public long limit() {
			return limit;
		}

		@Override
		public String toString() {
			return "WindowGroupedFlux[" + key + "]";
		}
	}

}

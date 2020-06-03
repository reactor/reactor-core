/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Combines the latest values from multiple sources through a function.
 *
 * @param <T> the value type of the sources
 * @param <R> the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxCombineLatest<T, R> extends Flux<R> implements Fuseable, SourceProducer<R> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	final Function<Object[], R> combiner;

	final Supplier<? extends Queue<SourceAndArray>> queueSupplier;

	final int prefetch;

	FluxCombineLatest(Publisher<? extends T>[] array,
			Function<Object[], R> combiner,
			Supplier<? extends Queue<SourceAndArray>> queueSupplier, int prefetch) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
	}

	FluxCombineLatest(Iterable<? extends Publisher<? extends T>> iterable,
			Function<Object[], R> combiner,
			Supplier<? extends Queue<SourceAndArray>> queueSupplier, int prefetch) {
		if (prefetch < 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		this.array = null;
		this.iterable = Objects.requireNonNull(iterable, "iterable");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super R> actual) {
		Publisher<? extends T>[] a = array;
		int n;
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			try {
				it = Objects.requireNonNull(iterable.iterator(), "The iterator returned is null");
			}
			catch (Throwable e) {
				Operators.error(actual, Operators.onOperatorError(e,
						actual.currentContext()));
				return;
			}

			for (; ; ) {

				boolean b;

				try {
					b = it.hasNext();
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (!b) {
					break;
				}

				Publisher<? extends T> p;

				try {
					p = Objects.requireNonNull(it.next(),
							"The Publisher returned by the iterator is null");
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (n == a.length) {
					Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, c, 0, n);
					a = c;
				}
				a[n++] = p;
			}

		}
		else {
			n = a.length;
		}

		if (n == 0) {
			Operators.complete(actual);
			return;
		}
		if (n == 1) {
			Function<T, R> f = t -> combiner.apply(new Object[]{t});
			if (a[0] instanceof Fuseable) {
				new FluxMapFuseable<>(from(a[0]), f).subscribe(actual);
				return;
			}
			else if (!(actual instanceof QueueSubscription)) {
				new FluxMap<>(from(a[0]), f).subscribe(actual);
				return;
			}
		}

		Queue<SourceAndArray> queue = queueSupplier.get();

		CombineLatestCoordinator<T, R> coordinator =
				new CombineLatestCoordinator<>(actual, combiner, n, queue, prefetch);

		actual.onSubscribe(coordinator);

		coordinator.subscribe(a, n);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return prefetch;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class CombineLatestCoordinator<T, R>
			implements QueueSubscription<R>, InnerProducer<R> {

		final Function<Object[], R>     combiner;
		final CombineLatestInner<T>[]   subscribers;
		final Queue<SourceAndArray>     queue;
		final Object[]                  latest;
		final CoreSubscriber<? super R> actual;

		boolean outputFused;

		int nonEmptySources;

		int completedSources;

		volatile boolean cancelled;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<CombineLatestCoordinator> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						"requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CombineLatestCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						"wip");

		volatile boolean done;

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestCoordinator, Throwable>
				ERROR =
				AtomicReferenceFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						Throwable.class,
						"error");

		CombineLatestCoordinator(CoreSubscriber<? super R> actual,
				Function<Object[], R> combiner,
				int n,
				Queue<SourceAndArray> queue, int prefetch) {
		 	this.actual = actual;
			this.combiner = combiner;
			@SuppressWarnings("unchecked") CombineLatestInner<T>[] a =
					new CombineLatestInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new CombineLatestInner<>(this, i, prefetch);
			}
			this.subscribers = a;
			this.latest = new Object[n];
			this.queue = queue;
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;
			cancelAll();

			if (WIP.getAndIncrement(this) == 0) {
				clear();
			}
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerProducer.super.scanUnsafe(key);
		}

		void subscribe(Publisher<? extends T>[] sources, int n) {
			CombineLatestInner<T>[] a = subscribers;

			for (int i = 0; i < n; i++) {
				if (done || cancelled) {
					return;
				}
				sources[i].subscribe(a[i]);
			}
		}

		void innerValue(int index, T value) {

			boolean replenishInsteadOfDrain;

			synchronized (this) {
				Object[] os = latest;

				int localNonEmptySources = nonEmptySources;

				if (os[index] == null) {
					localNonEmptySources++;
					nonEmptySources = localNonEmptySources;
				}

				os[index] = value;

				if (os.length == localNonEmptySources) {
					SourceAndArray sa =
							new SourceAndArray(subscribers[index], os.clone());

					if (!queue.offer(sa)) {
						innerError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), actual.currentContext()));
						return;
					}

					replenishInsteadOfDrain = false;
				}
				else {
					replenishInsteadOfDrain = true;
				}
			}

			if (replenishInsteadOfDrain) {
				subscribers[index].requestOne();
			}
			else {
				drain();
			}
		}

		void innerComplete(int index) {
			synchronized (this) {
				Object[] os = latest;

				if (os[index] != null) {
					int localCompletedSources = completedSources + 1;

					if (localCompletedSources == os.length) {
						done = true;
					}
					else {
						completedSources = localCompletedSources;
						return;
					}
				}
				else {
					done = true;
				}
			}
			drain();
		}

		void innerError(Throwable e) {

			if (Exceptions.addThrowable(ERROR, this, e)) {
				done = true;
				drain();
			}
			else {
				discardQueue(queue);
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		void drainOutput() {
			final CoreSubscriber<? super R> a = actual;
			final Queue<SourceAndArray> q = queue;

			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					discardQueue(q);
					return;
				}

				Throwable ex = error;
				if (ex != null) {
					discardQueue(q);
					a.onError(ex);
					return;
				}

				boolean d = done;

				boolean empty = q.isEmpty();

				if (!empty) {
					a.onNext(null);
				}

				if (d && empty) {
					a.onComplete();
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainAsync() {
			final Queue<SourceAndArray> q = queue;

			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (e != r) {
					boolean d = done;

					SourceAndArray v = q.poll();

					boolean empty = v == null;

					if (checkTerminated(d, empty, q)) {
						return;
					}

					if (empty) {
						break;
					}

					R w;

					try {
						w = Objects.requireNonNull(combiner.apply(v.array), "Combiner returned null");
					}
					catch (Throwable ex) {
						Context ctx = actual.currentContext();
						Operators.onDiscardMultiple(Stream.of(v.array), ctx);

						ex = Operators.onOperatorError(this, ex,	v.array, ctx);
						Exceptions.addThrowable(ERROR, this, ex);
						//noinspection ConstantConditions
						ex = Exceptions.terminate(ERROR, this);
						actual.onError(ex);
						return;
					}

					actual.onNext(w);

					v.source.requestOne();

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), q)) {
						return;
					}
				}

				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (outputFused) {
				drainOutput();
			}
			else {
				drainAsync();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Queue<SourceAndArray> q) {
			if (cancelled) {
				cancelAll();
				discardQueue(q);
				return true;
			}

			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);

				if (e != null && e != Exceptions.TERMINATED) {
					cancelAll();
					discardQueue(q);
					actual.onError(e);
					return true;
				}
				else if (empty) {
					cancelAll();

					actual.onComplete();
					return true;
				}
			}
			return false;
		}

		void cancelAll() {
			for (CombineLatestInner<T> inner : subscribers) {
				inner.cancel();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & THREAD_BARRIER) != 0) {
				return NONE;
			}
			int m = requestedMode & ASYNC;
			outputFused = m != 0;
			return m;
		}

		@Override
		@Nullable
		public R poll() {
			SourceAndArray e = queue.poll();
			if (e == null) {
				return null;
			}
			R r = combiner.apply(e.array);
			e.source.requestOne();
			return r;
		}

		private void discardQueue(Queue<SourceAndArray> q) {
			Operators.onDiscardQueueWithClear(q, actual.currentContext(), SourceAndArray::toStream);
		}

		@Override
		public void clear() {
			discardQueue(queue);
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	static final class CombineLatestInner<T>
			implements InnerConsumer<T> {

		final CombineLatestCoordinator<T, ?> parent;

		final int index;

		final int prefetch;

		final int limit;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(CombineLatestInner.class,
						Subscription.class,
						"s");

		int produced;

		 CombineLatestInner(CombineLatestCoordinator<T, ?> parent,
				int index,
				int prefetch) {
			this.parent = parent;
			this.index = index;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch);
		}
		
		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			parent.innerValue(index, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(index);
		}

		public void cancel() {
			Operators.terminate(S, this);
		}

		void requestOne() {
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			}
			else {
				produced = p;
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return  s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}

	/**
	 * The queue element type for internal use with FluxCombineLatest.
	 */
	static final class SourceAndArray {

		final CombineLatestInner<?> source;
		final Object[]              array;

		SourceAndArray(CombineLatestInner<?> source, Object[] array) {
			this.source = source;
			this.array = array;
		}

		final Stream<?> toStream() {
			return Stream.of(this.array);
		}
	}
}

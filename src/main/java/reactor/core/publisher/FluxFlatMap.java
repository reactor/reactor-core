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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;


/**
 * Maps a sequence of values each into a Publisher and flattens them
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFlatMap<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final boolean delayError;

	final int maxConcurrency;

	final Supplier<? extends Queue<R>> mainQueueSupplier;

	final int prefetch;

	final Supplier<? extends Queue<R>> innerQueueSupplier;

	FluxFlatMap(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError,
			int maxConcurrency,
			Supplier<? extends Queue<R>> mainQueueSupplier,
			int prefetch,
			Supplier<? extends Queue<R>> innerQueueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		if (maxConcurrency <= 0) {
			throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.maxConcurrency = maxConcurrency;
		this.mainQueueSupplier =
				Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.innerQueueSupplier =
				Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {

		if (trySubscribeScalarMap(source, s, mapper, false)) {
			return;
		}

		source.subscribe(new FlatMapMain<>(s,
				mapper,
				delayError,
				maxConcurrency,
				mainQueueSupplier,
				prefetch,
				innerQueueSupplier));
	}

	/**
	 * Checks if the source is a Supplier and if the mapper's publisher output is also
	 * a supplier, thus avoiding subscribing to any of them.
	 *
	 * @param source the source publisher
	 * @param s the end consumer
	 * @param mapper the mapper function
	 * @param fuseableExpected if true, the parent class was marked Fuseable thus the
	 * mapping output has to signal onSubscribe with a QueueSubscription
	 *
	 * @return true if the optimization worked
	 */
	@SuppressWarnings("unchecked")
	static <T, R> boolean trySubscribeScalarMap(Publisher<? extends T> source,
			Subscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean fuseableExpected) {
		if (source instanceof Callable) {
			T t;

			try {
				t = ((Callable<? extends T>) source).call();
			}
			catch (Throwable e) {
				Operators.error(s, Operators.onOperatorError(e));
				return true;
			}

			if (t == null) {
				Operators.complete(s);
				return true;
			}

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null Publisher");
			}
			catch (Throwable e) {
				Operators.error(s, Operators.onOperatorError(e));
				return true;
			}

			if (p instanceof Callable) {
				R v;

				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable e) {
					Operators.error(s, Operators.onOperatorError(e));
					return true;
				}

				if (v != null) {
					s.onSubscribe(Operators.scalarSubscription(s, v));
				}
				else {
					Operators.complete(s);
				}
			}
			else {
				if (!fuseableExpected || p instanceof Fuseable) {
					p.subscribe(s);
				}
				else {
					p.subscribe(new FluxHide.SuppressFuseableSubscriber<>(s));
				}
			}

			return true;
		}

		return false;
	}

	static final class FlatMapMain<T, R> extends FlatMapTracker<FlatMapInner<R>>
			implements InnerOperator<T, R> {

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final Subscriber<? super R> actual;

		final boolean delayError;

		final int maxConcurrency;

		final Supplier<? extends Queue<R>> mainQueueSupplier;

		final int prefetch;

		final Supplier<? extends Queue<R>> innerQueueSupplier;

		final int limit;

		volatile Queue<R> scalarQueue;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapMain.class,
						Throwable.class,
						"error");

		volatile boolean done;

		volatile boolean cancelled;

		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FlatMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FlatMapMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FlatMapMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FlatMapMain.class, "wip");

		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] EMPTY = new FlatMapInner[0];

		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] TERMINATED = new FlatMapInner[0];

		int lastIndex;

		int produced;

		FlatMapMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				boolean delayError,
				int maxConcurrency,
				Supplier<? extends Queue<R>> mainQueueSupplier,
				int prefetch,
				Supplier<? extends Queue<R>> innerQueueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.delayError = delayError;
			this.maxConcurrency = maxConcurrency;
			this.mainQueueSupplier = mainQueueSupplier;
			this.prefetch = prefetch;
			this.innerQueueSupplier = innerQueueSupplier;
			this.limit = maxConcurrency - (maxConcurrency >> 2);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(array).filter(Objects::nonNull);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.TERMINATED) return done && (scalarQueue == null || scalarQueue.isEmpty());
			if (key == BooleanAttr.DELAY_ERROR) return delayError;
			if (key == IntAttr.PREFETCH) return maxConcurrency;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == IntAttr.BUFFERED) return (scalarQueue != null ? scalarQueue.size() : 0) + size;

			return InnerOperator.super.scanUnsafe(key);
		}

		@SuppressWarnings("unchecked")
		@Override
		FlatMapInner<R>[] empty() {
			return EMPTY;
		}

		@SuppressWarnings("unchecked")
		@Override
		FlatMapInner<R>[] terminated() {
			return TERMINATED;
		}

		@SuppressWarnings("unchecked")
		@Override
		FlatMapInner<R>[] newArray(int size) {
			return new FlatMapInner[size];
		}

		@Override
		void setIndex(FlatMapInner<R> entry, int index) {
			entry.index = index;
		}

		@Override
		void unsubscribeEntry(FlatMapInner<R> entry) {
			entry.cancel();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public final Subscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				if (WIP.getAndIncrement(this) == 0) {
					scalarQueue = null;
					s.cancel();
					unsubscribe();
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				if (maxConcurrency == Integer.MAX_VALUE) {
					s.request(Long.MAX_VALUE);
				}
				else {
					s.request(maxConcurrency);
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
				"The mapper returned a null Publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			if (p instanceof Callable) {
				R v;
				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
				emitScalar(v);
			}
			else {
				FlatMapInner<R> inner = new FlatMapInner<>(this, prefetch);
				if (add(inner)) {

					p.subscribe(inner);
				}
			}

		}

		void emitScalar(R v) {
			if (v == null) {
				if (maxConcurrency != Integer.MAX_VALUE) {
					int p = produced + 1;
					if (p == limit) {
						produced = 0;
						s.request(p);
					}
					else {
						produced = p;
					}
				}
				return;
			}

			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != 0L) {
					actual.onNext(v);

					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}

					if (maxConcurrency != Integer.MAX_VALUE) {
						int p = produced + 1;
						if (p == limit) {
							produced = 0;
							s.request(p);
						}
						else {
							produced = p;
						}
					}
				}
				else {
					Queue<R> q = getOrCreateScalarQueue();

					if (!q.offer(v) && failOverflow(v, s)){
						done = true;
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();
			}
			else {
				Queue<R> q;

				q = getOrCreateScalarQueue();

				if (!q.offer(v) && failOverflow(v, s)) {
					done = true;
				}
				drain();
			}
		}

		Queue<R> getOrCreateScalarQueue() {
			Queue<R> q = scalarQueue;
			if (q == null) {
				q = mainQueueSupplier.get();
				scalarQueue = q;
			}
			return q;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
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
			if (done) {
				return;
			}

			done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			drainLoop();
		}

		void drainLoop() {
			int missed = 1;

			final Subscriber<? super R> a = actual;

			for (; ; ) {

				boolean d = done;

				FlatMapInner<R>[] as = get();

				int n = as.length;

				Queue<R> sq = scalarQueue;

				boolean noSources = isEmpty();

				if (checkTerminated(d, noSources && (sq == null || sq.isEmpty()), a)) {
					return;
				}

				boolean again = false;

				long r = requested;
				long e = 0L;
				long replenishMain = 0L;

				if (r != 0L && sq != null) {

					while (e != r) {
						d = done;

						R v = sq.poll();

						boolean empty = v == null;

						if (checkTerminated(d, false, a)) {
							return;
						}

						if (empty) {
							break;
						}

						a.onNext(v);

						e++;
					}

					if (e != 0L) {
						replenishMain += e;
						if (r != Long.MAX_VALUE) {
							r = Operators.addAndGet(REQUESTED, this, -e);
						}
						e = 0L;
						again = true;
					}
				}
				if (r != 0L && !noSources) {

					int j = lastIndex;
					//Do not need to wrap j since lastIndex is always 0..<n
//					if (j >= n) {
//						j = 0;
//					}

					for (int i = 0; i < n; i++) {
						if (cancelled) {
							scalarQueue = null;
							s.cancel();
							unsubscribe();
							return;
						}

						FlatMapInner<R> inner = as[j];
						if (inner != null) {
							d = inner.done;
							Queue<R> q = inner.queue;
							if (d && q == null) {
								remove(inner.index);
								again = true;
								replenishMain++;
							}
							else if (q != null) {
								while (e != r) {
									d = inner.done;

									R v;

									try {
										v = q.poll();
									}
									catch (Throwable ex) {
										ex = Operators.onOperatorError(inner, ex);
										if (!Exceptions.addThrowable(ERROR, this, ex)) {
											Operators.onErrorDropped(ex);
										}
										v = null;
										d = true;
									}

									boolean empty = v == null;

									if (checkTerminated(d, false, a)) {
										return;
									}

									if (d && empty) {
										remove(inner.index);
										again = true;
										replenishMain++;
										break;
									}

									if (empty) {
										break;
									}

									a.onNext(v);

									e++;
								}

								if (e == r) {
									d = inner.done;
									boolean empty = q.isEmpty();
									if (d && empty) {
										remove(inner.index);
										again = true;
										replenishMain++;
									}
								}

								if (e != 0L) {
									if (!inner.done) {
										inner.request(e);
									}
									if (r != Long.MAX_VALUE) {
										r = Operators.addAndGet(REQUESTED, this, -e);
										if (r == 0L) {
											break; // 0 .. n - 1
										}
									}
									e = 0L;
								}
							}
						}

						if (r == 0L) {
							break;
						}

						if (++j == n) {
							j = 0;
						}
					}

					lastIndex = j;
				}

				if (r == 0L && !noSources) {
					as = get();
					n = as.length;

					for (int i = 0; i < n; i++) {
						if (cancelled) {
							scalarQueue = null;
							s.cancel();
							unsubscribe();
							return;
						}

						FlatMapInner<R> inner = as[i];
						if (inner == null) {
							continue;
						}

						d = inner.done;
						Queue<R> q = inner.queue;
						boolean empty = (q == null || q.isEmpty());

						// if we have a non-empty source then quit the cleanup
						if (!empty) {
							break;
						}

						if (d && empty) {
							remove(inner.index);
							again = true;
							replenishMain++;
						}
					}
				}

				if (replenishMain != 0L && !done && !cancelled) {
					s.request(replenishMain);
				}

				if (again) {
					continue;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				scalarQueue = null;
				s.cancel();
				unsubscribe();

				return true;
			}

			if (delayError) {
				if (d && empty) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						a.onError(e);
					}
					else {
						a.onComplete();
					}

					return true;
				}
			}
			else {
				if (d) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						scalarQueue = null;
						s.cancel();
						unsubscribe();

						a.onError(e);
						return true;
					}
					else if (empty) {
						a.onComplete();
						return true;
					}
				}
			}

			return false;
		}

		void innerError(FlatMapInner<R> inner, Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				inner.done = true;
				if (!delayError) {
					done = true;
				}
				drain();
			}
			else {
				Operators.onErrorDropped(e);
			}
		}

		boolean failOverflow(R v, Subscription toCancel){
			Throwable e = Operators.onOperatorError(toCancel,
					Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
					v);

			if (!Exceptions.addThrowable(ERROR, this, e)) {
				Operators.onErrorDropped(e);
				return false;
			}
			return true;
		}

		void innerNext(FlatMapInner<R> inner, R v) {
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != 0L) {
					actual.onNext(v);

					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}

					inner.request(1);
				}
				else {
					Queue<R> q = getOrCreateInnerQueue(inner);

					if (!q.offer(v) && failOverflow(v, inner)){
						inner.done = true;
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();
			}
			else {
				Queue<R> q = getOrCreateInnerQueue(inner);

				if (!q.offer(v) && failOverflow(v, inner)) {
					inner.done = true;
				}
				drain();
			}
		}

		void innerComplete(FlatMapInner<R> inner) {
			//FIXME temp. reduce the case to empty regular inners
//			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
//				Queue<R> queue = inner.queue;
//				if (queue == null || queue.isEmpty()) {
//					remove(inner.index);
//
//					boolean d = done;
//					Queue<R> sq = scalarQueue;
//					boolean noSources = isEmpty();
//
//					if (checkTerminated(d,
//							noSources && (sq == null || sq.isEmpty()),
//							actual)) {
//						return;
//					}
//
//					s.request(1);
//					if (WIP.decrementAndGet(this) != 0) {
//						drainLoop();
//					}
//					return;
//				}
//			}
//			else {
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}
//			}
			drainLoop();
		}

		Queue<R> getOrCreateInnerQueue(FlatMapInner<R> inner) {
			Queue<R> q = inner.queue;
			if (q == null) {
				q = innerQueueSupplier.get();
				inner.queue = q;
			}
			return q;
		}

	}

	static final class FlatMapInner<R>
			implements InnerConsumer<R>, Subscription {

		final FlatMapMain<?, R> parent;

		final int prefetch;

		final int limit;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapInner.class,
						Subscription.class,
						"s");

		long produced;

		volatile Queue<R> queue;

		volatile boolean done;

		/**
		 * Represents the optimization mode of this inner subscriber.
		 */
		int sourceMode;

		int index;

		FlatMapInner(FlatMapMain<?, R> parent, int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<R> f =
							(Fuseable.QueueSubscription<R>) s;
					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;
						parent.drain();
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					}
					// NONE is just fall-through as the queue will be created on demand
				}
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(R t) {
			if (sourceMode == Fuseable.ASYNC) {
				parent.drain();
			}
			else {
				parent.innerNext(this, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			done = true;
			parent.innerError(this, t);
		}

		@Override
		public void onComplete() {
			// onComplete is practically idempotent so there is no risk due to subscription-race in async mode
			done = true;
			parent.innerComplete(this);
		}

		@Override
		public void request(long n) {
			long p = produced + n;
			if (p >= limit) {
				produced = 0L;
				s.request(p);
			}
			else {
				produced = p;
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == BooleanAttr.TERMINATED) return done && (queue == null || queue.isEmpty());
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == IntAttr.BUFFERED) return queue == null ? 0 : queue.size();
			if (key == IntAttr.PREFETCH) return prefetch;

			return null;
		}
	}
}

abstract class FlatMapTracker<T> {

	volatile T[] array = empty();

	int[] free = FREE_EMPTY;

	long producerIndex;
	long consumerIndex;

	volatile int size;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<FlatMapTracker> SIZE =
			AtomicIntegerFieldUpdater.newUpdater(FlatMapTracker.class, "size");

	static final int[] FREE_EMPTY = new int[0];

	abstract T[] empty();

	abstract T[] terminated();

	abstract T[] newArray(int size);

	abstract void unsubscribeEntry(T entry);

	abstract void setIndex(T entry, int index);

	final void unsubscribe() {
		T[] a;
		T[] t = terminated();
		synchronized (this) {
			a = array;
			if (a == t) {
				return;
			}
			SIZE.lazySet(this, 0);
			free = null;
			array = t;
		}
		for (T e : a) {
			if (e != null) {
				unsubscribeEntry(e);
			}
		}
	}

	final T[] get() {
		return array;
	}

	final boolean add(T entry) {
		T[] a = array;
		if (a == terminated()) {
			return false;
		}
		synchronized (this) {
			a = array;
			if (a == terminated()) {
				return false;
			}

			int idx = pollFree();
			if (idx < 0) {
				int n = a.length;
				T[] b = n != 0 ? newArray(n << 1) : newArray(4);
				System.arraycopy(a, 0, b, 0, n);

				array = b;
				a = b;

				int m = b.length;
				int[] u = new int[m];
				for (int i = n + 1; i < m; i++) {
					u[i] = i;
				}
				free = u;
				consumerIndex = n + 1;
				producerIndex = m;

				idx = n;
			}
			setIndex(entry, idx);
			SIZE.lazySet(this, size); // make sure entry is released
			a[idx] = entry;
			SIZE.lazySet(this, size + 1);
		}
		return true;
	}

	final void remove(int index) {
		synchronized (this) {
			T[] a = array;
			if (a != terminated()) {
				a[index] = null;
				offerFree(index);
				SIZE.lazySet(this, size - 1);
			}
		}
	}

	int pollFree() {
		int[] a = free;
		int m = a.length - 1;
		long ci = consumerIndex;
		if (producerIndex == ci) {
			return -1;
		}
		int offset = (int) ci & m;
		consumerIndex = ci + 1;
		return a[offset];
	}

	void offerFree(int index) {
		int[] a = free;
		int m = a.length - 1;
		long pi = producerIndex;
		int offset = (int) pi & m;
		a[offset] = index;
		producerIndex = pi + 1;
	}

	final boolean isEmpty() {
		return size == 0;
	}
}


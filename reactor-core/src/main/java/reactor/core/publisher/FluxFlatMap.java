/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Maps a sequence of values each into a Publisher and flattens them
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFlatMap<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final boolean delayError;

	final int maxConcurrency;

	final Supplier<? extends Queue<R>> mainQueueSupplier;

	final int prefetch;

	final Supplier<? extends Queue<R>> innerQueueSupplier;

	FluxFlatMap(Flux<? extends T> source,
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
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		if (trySubscribeScalarMap(source, actual, mapper, false, true)) {
			return null;
		}

		return new FlatMapMain<>(actual,
				mapper,
				delayError,
				maxConcurrency,
				mainQueueSupplier,
				prefetch, innerQueueSupplier);
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
			CoreSubscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean fuseableExpected,
			boolean errorContinueExpected) {
		if (source instanceof Callable) {
			T t;

			try {
				t = ((Callable<? extends T>) source).call();
			}
			catch (Throwable e) {
				Context ctx = s.currentContext();
				Throwable e_ = errorContinueExpected ?
					Operators.onNextError(null, e, ctx) :
					Operators.onOperatorError(e, ctx);
				if (e_ != null) {
					Operators.error(s, e_);
				}
				else {
					//the error was recovered but we know there won't be any more value
					Operators.complete(s);
				}
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
				Context ctx = s.currentContext();
				Throwable e_ = errorContinueExpected ?
						Operators.onNextError(t, e, ctx) :
						Operators.onOperatorError(null, e, t, ctx);
				if (e_ != null) {
					Operators.error(s, e_);
				}
				else {
					//the error was recovered but we know there won't be any more value
					Operators.complete(s);
				}
				return true;
			}

			if (p instanceof Callable) {
				R v;

				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable e) {
					Context ctx = s.currentContext();
					Throwable e_ = errorContinueExpected ?
							Operators.onNextError(t, e, ctx) :
							Operators.onOperatorError(null, e, t, ctx);
					if (e_ != null) {
						Operators.error(s, e_);
					}
					else {
						//the error was recovered but we know there won't be any more value
						Operators.complete(s);
					}
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

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FlatMapMain<T, R> extends FlatMapTracker<FlatMapInner<R>>
			implements InnerOperator<T, R> {

		final boolean                                               delayError;
		final int                                                   maxConcurrency;
		final int                                                   prefetch;
		final int                                                   limit;
		final Function<? super T, ? extends Publisher<? extends R>> mapper;
		final Supplier<? extends Queue<R>>                          mainQueueSupplier;
		final Supplier<? extends Queue<R>>                          innerQueueSupplier;
		final CoreSubscriber<? super R>                             actual;

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

		FlatMapMain(CoreSubscriber<? super R> actual,
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
			this.limit = Operators.unboundedOrLimit(maxConcurrency);
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(array).filter(Objects::nonNull);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return done && (scalarQueue == null || scalarQueue.isEmpty());
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.PREFETCH) return maxConcurrency;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.LARGE_BUFFERED) return (scalarQueue != null ? (long) scalarQueue.size() : 0L) + size;
			if (key == Attr.BUFFERED) {
				long realBuffered = (scalarQueue != null ? (long) scalarQueue.size() : 0L) + size;
				if (realBuffered <= Integer.MAX_VALUE) return (int) realBuffered;
				return Integer.MIN_VALUE;
			}
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
				Operators.addCap(REQUESTED, this, n);
				drain(null);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				if (WIP.getAndIncrement(this) == 0) {
					Operators.onDiscardQueueWithClear(scalarQueue, actual.currentContext(), null);
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
				s.request(Operators.unboundedOrPrefetch(maxConcurrency));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
				"The mapper returned a null Publisher");
			}
			catch (Throwable e) {
				Context ctx = actual.currentContext();
				Throwable e_ = Operators.onNextError(t, e, ctx, s);
				Operators.onDiscard(t, ctx);
				if (e_ != null) {
					onError(e_);
				}
				else {
					tryEmitScalar(null);
				}
				return;
			}

			if (p instanceof Callable) {
				R v;
				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable e) {
					Context ctx = actual.currentContext();
					//does the strategy apply? if so, short-circuit the delayError. In any case, don't cancel
					Throwable e_ = Operators.onNextError(t, e, ctx);
					if (e_ == null) {
						tryEmitScalar(null);
					}
					else if (!delayError || !Exceptions.addThrowable(ERROR, this, e_)) {
					//now if error mode strategy doesn't apply, let delayError play
						onError(Operators.onOperatorError(s, e_, t, ctx));
					}
					Operators.onDiscard(t, ctx);
					return;
				}
				tryEmitScalar(v);
			}
			else {
				FlatMapInner<R> inner = new FlatMapInner<>(this, prefetch);
				if (add(inner)) {
					p.subscribe(inner);
				} else {
					Operators.onDiscard(t, actual.currentContext());
				}
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
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain(null);
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;
			drain(null);
		}

		void tryEmitScalar(@Nullable R v) {
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

				Queue<R> q = scalarQueue;
				if (r != 0L && (q == null || q.isEmpty())) {
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
					if (q == null) {
						q = getOrCreateScalarQueue();
					}

					if (!q.offer(v) && failOverflow(v, s)){
						done = true;
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					if (cancelled) {
						Operators.onDiscard(v, actual.currentContext());
					}
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
				drain(v);
			}
		}

		void tryEmit(FlatMapInner<R> inner, R v) {
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				Queue<R> q = inner.queue;
				if (r != 0 && (q == null || q.isEmpty())) {
					actual.onNext(v);

					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}

					inner.request(1);
				}
				else {
					if (q == null) {
						q = getOrCreateInnerQueue(inner);
					}

					if (!q.offer(v) && failOverflow(v, inner)){
						inner.done = true;
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					if (cancelled) {
						Operators.onDiscard(v, actual.currentContext());
					}
					return;
				}

				drainLoop();
			}
			else {
				Queue<R> q = getOrCreateInnerQueue(inner);

				if (!q.offer(v) && failOverflow(v, inner)) {
					inner.done = true;
				}
				drain(v);
			}
		}

		void drain(@Nullable R dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (dataSignal != null && cancelled) {
					Operators.onDiscard(dataSignal, actual.currentContext());
				}
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

				if (checkTerminated(d, noSources && (sq == null || sq.isEmpty()), a, null)) {
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

						if (checkTerminated(d, false, a, v)) {
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
							r = REQUESTED.addAndGet(this, -e);
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
							Operators.onDiscardQueueWithClear(scalarQueue, actual.currentContext(), null);
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
										ex = Operators.onOperatorError(inner, ex,
												actual.currentContext());
										if (!Exceptions.addThrowable(ERROR, this, ex)) {
											Operators.onErrorDropped(ex,
													actual.currentContext());
										}
										v = null;
										d = true;
									}

									boolean empty = v == null;

									if (checkTerminated(d, false, a, v)) {
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
										r = REQUESTED.addAndGet(this, -e);
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
							Operators.onDiscardQueueWithClear(scalarQueue, actual.currentContext(), null);
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

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, @Nullable R value) {
			if (cancelled) {
				Context ctx = actual.currentContext();
				Operators.onDiscard(value, ctx);
				Operators.onDiscardQueueWithClear(scalarQueue, ctx, null);
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
						Context ctx = actual.currentContext();
						Operators.onDiscard(value, ctx);
						Operators.onDiscardQueueWithClear(scalarQueue, ctx, null);
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
			e = Operators.onNextInnerError(e, currentContext(), s);
			if (e != null) {
				if (Exceptions.addThrowable(ERROR, this, e)) {
					if (!delayError) {
						done = true;
					}
					inner.done = true;
					drain(null);
				}
				else {
					inner.done = true;
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
			else {
				inner.done = true;
				drain(null);
			}
		}

		boolean failOverflow(R v, Subscription toCancel){
			Throwable e = Operators.onOperatorError(toCancel,
					Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
					v, actual.currentContext());

			Operators.onDiscard(v, actual.currentContext());

			if (!Exceptions.addThrowable(ERROR, this, e)) {
				Operators.onErrorDropped(e, actual.currentContext());
				return false;
			}
			return true;
		}

		void innerComplete(FlatMapInner<R> inner) {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
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
//			this.limit = prefetch >> 2;
			this.limit = Operators.unboundedOrLimit(prefetch);
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
						parent.drain(null);
						return;
					}
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					}
					// NONE is just fall-through as the queue will be created on demand
				}
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(R t) {
			if (sourceMode == Fuseable.ASYNC) {
				parent.drain(t);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, parent.currentContext());
					return;
				}

				if (s == Operators.cancelledSubscription()) {
					Operators.onDiscard(t, parent.currentContext());
					return;
				}

				parent.tryEmit(this, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(this, t); //we want to delay the marking as done
		}

		@Override
		public void onComplete() {
			// onComplete is practically idempotent so there is no risk due to subscription-race in async mode
			done = true;
			parent.innerComplete(this);
		}

		@Override
		public void request(long n) {
			if (sourceMode == Fuseable.SYNC) {
				return;
			}
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
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
			Operators.onDiscardQueueWithClear(queue, parent.currentContext(), null);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.TERMINATED) return done && (queue == null || queue.isEmpty());
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.BUFFERED) return queue == null ? 0 : queue.size();
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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


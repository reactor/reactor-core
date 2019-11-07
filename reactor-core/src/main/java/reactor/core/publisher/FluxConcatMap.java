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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Exceptions.TERMINATED;

/**
 * Maps each upstream value into a Publisher and concatenates them into one
 * sequence of items.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxConcatMap<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final Supplier<? extends Queue<T>> queueSupplier;

	final int prefetch;

	final ErrorMode errorMode;

	/**
	 * Indicates when an error from the main source should be reported.
	 */
	enum ErrorMode {
		/**
		 * Report the error immediately, cancelling the active inner source.
		 */
		IMMEDIATE, /**
		 * Report error after an inner source terminated.
		 */
		BOUNDARY, /**
		 * Report the error after all sources terminated.
		 */
		END
	}

	static <T, R> CoreSubscriber<T> subscriber(CoreSubscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			Supplier<? extends Queue<T>> queueSupplier,
			int prefetch, ErrorMode errorMode) {
		switch (errorMode) {
			case BOUNDARY:
				return new ConcatMapDelayed<>(s,
						mapper,
						queueSupplier,
						prefetch,
						false);
			case END:
				return new ConcatMapDelayed<>(s,
						mapper,
						queueSupplier,
						prefetch,
						true);
			default:
				return new ConcatMapImmediate<>(s, mapper, queueSupplier, prefetch);
		}
	}

	FluxConcatMap(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			Supplier<? extends Queue<T>> queueSupplier,
			int prefetch,
			ErrorMode errorMode) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
		this.errorMode = Objects.requireNonNull(errorMode, "errorMode");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, true)) {
			return null;
		}

		return subscriber(actual, mapper, queueSupplier, prefetch, errorMode);
	}

	static final class ConcatMapImmediate<T, R>
			implements FluxConcatMapSupport<T, R> {

		final CoreSubscriber<? super R> actual;
		final Context ctx;

		final ConcatMapInner<R> inner;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final Supplier<? extends Queue<T>> queueSupplier;

		final int prefetch;

		final int limit;

		Subscription s;

		int consumed;

		volatile Queue<T> queue;

		volatile boolean done;

		volatile boolean cancelled;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ConcatMapImmediate, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ConcatMapImmediate.class,
						Throwable.class,
						"error");

		volatile boolean active;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatMapImmediate> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ConcatMapImmediate.class, "wip");

		volatile int guard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatMapImmediate> GUARD =
				AtomicIntegerFieldUpdater.newUpdater(ConcatMapImmediate.class, "guard");

		int sourceMode;

		ConcatMapImmediate(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				Supplier<? extends Queue<T>> queueSupplier, int prefetch) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.mapper = mapper;
			this.queueSupplier = queueSupplier;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch);
			this.inner = new ConcatMapInner<>(this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done || error == TERMINATED;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.ERROR) return error;

			return FluxConcatMapSupport.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
							(Fuseable.QueueSubscription<T>) s;
					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);

						drain();
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					}
					else {
						queue = queueSupplier.get();
					}
				}
				else {
					queue = queueSupplier.get();
				}

				actual.onSubscribe(this);

				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				drain();
			}
			else if (!queue.offer(t)) {
				onError(Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						this.ctx));
				Operators.onDiscard(t, this.ctx);
			}
			else {
				drain();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				inner.cancel();

				if (GUARD.getAndIncrement(this) == 0) {
					t = Exceptions.terminate(ERROR, this);
					if (t != TERMINATED) {
						actual.onError(t);
						Operators.onDiscardQueueWithClear(queue, this.ctx, null);
					}
				}
			}
			else {
				Operators.onErrorDropped(t, this.ctx);
			}
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		@Override
		public void innerNext(R value) {
			if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
				actual.onNext(value);
				if (GUARD.compareAndSet(this, 1, 0)) {
					return;
				}
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != TERMINATED) {
					actual.onError(e);
				}
			}
		}

		@Override
		public void innerComplete() {
			active = false;
			drain();
		}

		@Override
		public void innerError(Throwable e) {
			e = Operators.onNextInnerError(e, currentContext(), s);
			if(e != null) {
				if (Exceptions.addThrowable(ERROR, this, e)) {
					s.cancel();

					if (GUARD.getAndIncrement(this) == 0) {
						e = Exceptions.terminate(ERROR, this);
						if (e != TERMINATED) {
							actual.onError(e);
						}
					}
				}
				else {
					Operators.onErrorDropped(e, this.ctx);
				}
			}
			else {
				active = false;
				drain();
			}
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			inner.request(n);
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				inner.cancel();
				s.cancel();
				Operators.onDiscardQueueWithClear(queue, this.ctx, null);
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) == 0) {
				for (; ; ) {
					if (cancelled) {
						return;
					}

					if (!active) {
						boolean d = done;

						T v;

						try {
							v = queue.poll();
						}
						catch (Throwable e) {
							actual.onError(Operators.onOperatorError(s, e, this.ctx));
							return;
						}

						boolean empty = v == null;

						if (d && empty) {
							actual.onComplete();
							return;
						}

						if (!empty) {
							Publisher<? extends R> p;

							try {
								p = Objects.requireNonNull(mapper.apply(v),
								"The mapper returned a null Publisher");
							}
							catch (Throwable e) {
								Operators.onDiscard(v, this.ctx);
								Throwable e_ = Operators.onNextError(v, e, this.ctx, s);
								if (e_ != null) {
									actual.onError(Operators.onOperatorError(s, e, v,
											this.ctx));
									return;
								}
								else {
									continue;
								}
							}

							if (sourceMode != Fuseable.SYNC) {
								int c = consumed + 1;
								if (c == limit) {
									consumed = 0;
									s.request(c);
								}
								else {
									consumed = c;
								}
							}

							if (p instanceof Callable) {
								@SuppressWarnings("unchecked") Callable<R> callable =
										(Callable<R>) p;

								R vr;

								try {
									vr = callable.call();
								}
								catch (Throwable e) {
									Throwable e_ = Operators.onNextError(v, e, this.ctx, s);
									if (e_ != null) {
										actual.onError(Operators.onOperatorError(s, e, v,
												this.ctx));
										return;
									}
									else {
										continue;
									}
								}

								if (vr == null) {
									continue;
								}

								if (inner.isUnbounded()) {
									if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
										actual.onNext(vr);
										if (!GUARD.compareAndSet(this, 1, 0)) {
											Throwable e =
													Exceptions.terminate(ERROR, this);
											if (e != TERMINATED) {
												actual.onError(e);
											}
											return;
										}
									}
									continue;
								}
								else {
									active = true;
									inner.set(new WeakScalarSubscription<>(vr, inner));
								}

							}
							else {
								active = true;
								p.subscribe(inner);
							}
						}
					}
					if (WIP.decrementAndGet(this) == 0) {
						break;
					}
				}
			}
		}
	}

	static final class WeakScalarSubscription<T> implements Subscription {

		final CoreSubscriber<? super T> actual;
		final T                     value;
		boolean once;

		WeakScalarSubscription(T value, CoreSubscriber<? super T> actual) {
			this.value = value;
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			if (n > 0 && !once) {
				once = true;
				Subscriber<? super T> a = actual;
				a.onNext(value);
				a.onComplete();
			}
		}

		@Override
		public void cancel() {
			Operators.onDiscard(value, actual.currentContext());
		}
	}

	static final class ConcatMapDelayed<T, R>
			implements FluxConcatMapSupport<T, R> {

		final CoreSubscriber<? super R> actual;

		final ConcatMapInner<R> inner;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final Supplier<? extends Queue<T>> queueSupplier;

		final int prefetch;

		final int limit;

		final boolean veryEnd;

		Subscription s;

		int consumed;

		volatile Queue<T> queue;

		volatile boolean done;

		volatile boolean cancelled;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ConcatMapDelayed, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ConcatMapDelayed.class,
						Throwable.class,
						"error");

		volatile boolean active;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatMapDelayed> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ConcatMapDelayed.class, "wip");

		int sourceMode;

		ConcatMapDelayed(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				Supplier<? extends Queue<T>> queueSupplier,
				int prefetch, boolean veryEnd) {
			this.actual = actual;
			this.mapper = mapper;
			this.queueSupplier = queueSupplier;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch);
			this.veryEnd = veryEnd;
			this.inner = new ConcatMapInner<>(this);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return true;

			return FluxConcatMapSupport.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
							(Fuseable.QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);

						drain();
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					}
					else {
						queue = queueSupplier.get();
					}
				}
				else {
					queue = queueSupplier.get();
				}

				actual.onSubscribe(this);

				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				drain();
			}
			else if (!queue.offer(t)) {
				Context ctx = actual.currentContext();
				onError(Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						ctx));
				Operators.onDiscard(t, ctx);
			}
			else {
				drain();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		@Override
		public void innerNext(R value) {
			actual.onNext(value);
		}

		@Override
		public void innerComplete() {
			active = false;
			drain();
		}

		@Override
		public void innerError(Throwable e) {
			e = Operators.onNextInnerError(e, currentContext(), s);
			if(e != null) {
				if (Exceptions.addThrowable(ERROR, this, e)) {
					if (!veryEnd) {
						s.cancel();
						done = true;
					}
					active = false;
					drain();
				}
				else {
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
			else {
				active = false;
			}
		}

		@Override
		public void request(long n) {
			inner.request(n);
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				inner.cancel();
				s.cancel();
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) == 0) {
				Context ctx = null;
				for (; ; ) {
					if (cancelled) {
						return;
					}

					if (!active) {

						boolean d = done;

						if (d && !veryEnd) {
							Throwable ex = error;
							if (ex != null) {
								ex = Exceptions.terminate(ERROR, this);
								if (ex != TERMINATED) {
									actual.onError(ex);
								}
								return;
							}
						}

						T v;

						try {
							v = queue.poll();
						}
						catch (Throwable e) {
							actual.onError(Operators.onOperatorError(s, e, actual.currentContext()));
							return;
						}

						boolean empty = v == null;

						if (d && empty) {
							Throwable ex = Exceptions.terminate(ERROR, this);
							if (ex != null && ex != TERMINATED) {
								actual.onError(ex);
							}
							else {
								actual.onComplete();
							}
							return;
						}

						if (!empty) {
							Publisher<? extends R> p;

							try {
								p = Objects.requireNonNull(mapper.apply(v),
										"The mapper returned a null Publisher");
							}
							catch (Throwable e) {
								if (ctx == null) {
									ctx = actual.currentContext();
								}
								Operators.onDiscard(v, ctx);
								Throwable e_ = Operators.onNextError(v, e, ctx, s);
								if (e_ != null) {
									actual.onError(Operators.onOperatorError(s, e, v, ctx));
									return;
								}
								else {
									continue;
								}
							}

							if (sourceMode != Fuseable.SYNC) {
								int c = consumed + 1;
								if (c == limit) {
									consumed = 0;
									s.request(c);
								}
								else {
									consumed = c;
								}
							}

							if (p instanceof Callable) {
								@SuppressWarnings("unchecked") Callable<R> supplier =
										(Callable<R>) p;

								R vr;

								try {
									vr = supplier.call();
								}
								catch (Throwable e) {
									//does the strategy apply? if so, short-circuit the delayError. In any case, don't cancel
									if (ctx == null) {
										ctx = actual.currentContext();
									}
									Throwable e_ = Operators.onNextError(v, e, ctx);
									if (e_ == null) {
										continue;
									}
									//now if error mode strategy doesn't apply, let delayError play
									if (veryEnd && Exceptions.addThrowable(ERROR, this, e_)) {
										continue;
									}
									else {
										actual.onError(Operators.onOperatorError(s, e_, v, ctx));
										return;
									}
								}

								if (vr == null) {
									continue;
								}

								if (inner.isUnbounded()) {
									actual.onNext(vr);
									continue;
								}
								else {
									active = true;
									inner.set(new WeakScalarSubscription<>(vr, inner));
								}
							}
							else {
								active = true;
								p.subscribe(inner);
							}
						}
					}
					if (WIP.decrementAndGet(this) == 0) {
						break;
					}
				}
			}
		}
	}

	/**
	 * @param <I> input type consumed by the InnerOperator
	 * @param <T> output type, as forwarded by the inner this helper supports
	 */
	interface FluxConcatMapSupport<I, T> extends InnerOperator<I, T> {

		void innerNext(T value);

		void innerComplete();

		void innerError(Throwable e);
	}

	static final class ConcatMapInner<R>
			extends Operators.MultiSubscriptionSubscriber<R, R> {

		final FluxConcatMapSupport<?, R> parent;

		long produced;

		ConcatMapInner(FluxConcatMapSupport<?, R> parent) {
			super(Operators.emptySubscriber());
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return parent;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(R t) {
			produced++;

			parent.innerNext(t);
		}

		@Override
		public void onError(Throwable t) {
			long p = produced;

			if (p != 0L) {
				produced = 0L;
				produced(p);
			}

			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			long p = produced;

			if (p != 0L) {
				produced = 0L;
				produced(p);
			}

			parent.innerComplete();
		}
	}
}

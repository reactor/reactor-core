/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxConcatMap.ConcatMapInner;
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.core.publisher.FluxConcatMap.FluxConcatMapSupport;
import reactor.core.publisher.FluxConcatMap.WeakScalarSubscription;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Maps each upstream value into a Publisher and concatenates them into one
 * sequence of items.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see FluxConcatMap
 */
final class FluxConcatMapNoPrefetch<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final ErrorMode errorMode;

	FluxConcatMapNoPrefetch(
			Flux<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			ErrorMode errorMode
	) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.errorMode = errorMode;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, true)) {
			return null;
		}

		return new FluxConcatMapNoPrefetchSubscriber<>(actual, mapper, errorMode);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return 0;
	}

	static final class FluxConcatMapNoPrefetchSubscriber<T, R> implements FluxConcatMapSupport<T, R> {

		enum State {
			INITIAL,
			/**
			 * Requested from {@link #upstream}, waiting for {@link #onNext(Object)}
			 */
			REQUESTED,
			/**
			 * {@link #onNext(Object)} received, listening on {@link #inner}
			 */
			ACTIVE,
			/**
			 * Received outer {@link #onComplete()}, waiting for {@link #inner} to complete
			 */
			LAST_ACTIVE,
			/**
			 * Terminated either successfully or after an error
			 */
			TERMINATED,
			CANCELLED,
		}

		volatile State state;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FluxConcatMapNoPrefetchSubscriber, State> STATE = AtomicReferenceFieldUpdater.newUpdater(
				FluxConcatMapNoPrefetchSubscriber.class,
				State.class,
				"state"
		);

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FluxConcatMapNoPrefetchSubscriber, Throwable> ERROR = AtomicReferenceFieldUpdater.newUpdater(
				FluxConcatMapNoPrefetchSubscriber.class,
				Throwable.class,
				"error"
		);

		final CoreSubscriber<? super R> actual;

		final ConcatMapInner<R> inner;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final ErrorMode errorMode;

		Subscription upstream;

		FluxConcatMapNoPrefetchSubscriber(
				CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				ErrorMode errorMode
		) {
			this.actual = actual;
			this.mapper = mapper;
			this.errorMode = errorMode;
			this.inner = new ConcatMapInner<>(this);
			STATE.lazySet(this, State.INITIAL);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return upstream;
			if (key == Attr.TERMINATED) return state == State.TERMINATED;
			if (key == Attr.CANCELLED) return state == State.CANCELLED;
			if (key == Attr.DELAY_ERROR) return this.errorMode != ErrorMode.IMMEDIATE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return FluxConcatMapSupport.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.upstream, s)) {
				this.upstream = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!STATE.compareAndSet(this, State.REQUESTED, State.ACTIVE)) {
				switch (state) {
					case CANCELLED:
						Operators.onDiscard(t, currentContext());
						break;
					case TERMINATED:
						Operators.onNextDropped(t, currentContext());
						break;
				}
				return;
			}

			try {
				Publisher<? extends R> p = mapper.apply(t);
				Objects.requireNonNull(p, "The mapper returned a null Publisher");

				if (p instanceof Callable) {
					@SuppressWarnings("unchecked")
					Callable<R> callable = (Callable<R>) p;

					R result = callable.call();
					if (result == null) {
						innerComplete();
						return;
					}

					if (inner.isUnbounded()) {
						actual.onNext(result);
						innerComplete();
						return;
					}

					inner.set(new WeakScalarSubscription<>(result, inner));
					return;
				}

				p.subscribe(inner);
			}
			catch (Throwable e) {
				Context ctx = actual.currentContext();
				Operators.onDiscard(t, ctx);
				if (!maybeOnError(Operators.onNextError(t, e, ctx), ctx, upstream)) {
					innerComplete();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			Context ctx = currentContext();
			if (!maybeOnError(t, ctx, inner)) {
				onComplete();
			}
		}

		@Override
		public void onComplete() {
			for (State previousState = this.state; ; previousState = this.state) {
				switch (previousState) {
					case INITIAL:
					case REQUESTED:
						if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
							continue;
						}

						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
							return;
						}
						actual.onComplete();
						return;
					case ACTIVE:
						if (!STATE.compareAndSet(this, previousState, State.LAST_ACTIVE)) {
							continue;
						}
						return;
					default:
						return;
				}
			}
		}

		@Override
		public synchronized void innerNext(R value) {
			switch (state) {
				case ACTIVE:
				case LAST_ACTIVE:
					actual.onNext(value);
					break;
				default:
					Operators.onDiscard(value, currentContext());
					break;
			}
		}

		@Override
		public void innerComplete() {
			for (State previousState = this.state; ; previousState = this.state) {
				switch (previousState) {
					case ACTIVE:
						if (!STATE.compareAndSet(this, previousState, State.REQUESTED)) {
							continue;
						}
						upstream.request(1);
						return;
					case LAST_ACTIVE:
						if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
							continue;
						}

						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
							return;
						}
						actual.onComplete();
						return;
					default:
						return;
				}
			}
		}

		@Override
		public void innerError(Throwable e) {
			Context ctx = currentContext();
			if (!maybeOnError(Operators.onNextInnerError(e, ctx, null), ctx, upstream)) {
				innerComplete();
			}
		}

		private boolean maybeOnError(@Nullable Throwable e, Context ctx, Subscription subscriptionToCancel) {
			if (e == null) {
				return false;
			}

			if (!ERROR.compareAndSet(this, null, e)) {
				Operators.onErrorDropped(e, ctx);
			}

			if (errorMode == ErrorMode.END) {
				return false;
			}

			for (State previousState = this.state; ; previousState = this.state) {
				switch (previousState) {
					case CANCELLED:
					case TERMINATED:
						return true;
					default:
						if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
							continue;
						}
						subscriptionToCancel.cancel();
						synchronized (this) {
							actual.onError(error);
						}
						return true;
				}
			}
		}

		@Override
		public void request(long n) {
			if (STATE.compareAndSet(this, State.INITIAL, State.REQUESTED)) {
				upstream.request(1);
			}
			inner.request(n);
		}

		@Override
		public void cancel() {
			switch (STATE.getAndSet(this, State.CANCELLED)) {
				case CANCELLED:
					break;
				case TERMINATED:
					inner.cancel();
					break;
				default:
					inner.cancel();
					upstream.cancel();
			}
		}
	}
}

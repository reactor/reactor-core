/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * In case of onError, complete the sequence with a fallback value or simply with the onComplete signal in case said value is null.
 * Only consider {@link Throwable} that match the provided {@link Predicate}, if any.
 * <p>
 * This operator is behind both {@link Flux#onErrorReturn(Object)} and {@link Flux#onErrorComplete()} APIs.
 *
 * @param <T> the value type
 */
final class FluxOnErrorReturn<T> extends InternalFluxOperator<T, T> {

	@Nullable
	final Predicate<? super Throwable> resumableErrorPredicate;

	@Nullable
	final T fallbackValue;

	FluxOnErrorReturn(Flux<? extends T> source, @Nullable Predicate<? super Throwable> predicate, @Nullable T value) {
		super(source);
		resumableErrorPredicate = predicate;
		fallbackValue = value;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new ReturnSubscriber<>(actual, resumableErrorPredicate, fallbackValue, false);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ReturnSubscriber<T> implements InnerOperator<T, T> {

		private static final byte STATE_CANCELLED = -3;
		private static final byte STATE_TERMINATED = -2;
		private static final byte STATE_PENDING_FALLBACK = -1;

		final CoreSubscriber<? super T> actual;
		final boolean                   trackRequestWhenFallbackDeferred;

		Subscription s;

		@Nullable
		final Predicate<? super Throwable> resumableErrorPredicate;

		@Nullable
		final T fallbackValue;

		volatile     long                                     requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReturnSubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(ReturnSubscriber.class, "requested");


		ReturnSubscriber(CoreSubscriber<? super T> actual, @Nullable Predicate<? super Throwable> predicate,
						 @Nullable T value, boolean trackRequestWhenFallbackDeferred) {
			this.actual = actual;
			resumableErrorPredicate = predicate;
			fallbackValue = value;
			this.trackRequestWhenFallbackDeferred = trackRequestWhenFallbackDeferred;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		void producedOne() {
			long r;
			for (;;) {
				r = REQUESTED.get(this);
				if (r == Long.MAX_VALUE || r <= 0) {
					return;
				}

				if (REQUESTED.compareAndSet(this, r, r - 1)) {
					return;
				}
			}
		}

		@Override
		public void cancel() {
			if (REQUESTED.getAndSet(this, STATE_CANCELLED) != STATE_CANCELLED) {
				s.cancel();
			}
		}

		@Override
		public void request(long n) {
			long r;
			for (;;) {
				r = REQUESTED.get(this);
				if (r == Long.MAX_VALUE || r < STATE_PENDING_FALLBACK) {
					return;
				}

				if (r == STATE_PENDING_FALLBACK) {
					//ensure no concurrent request took care of it already
					if (REQUESTED.compareAndSet(this, r, STATE_TERMINATED)) {
						if (this.fallbackValue != null) { //should be always true at this point
							actual.onNext(this.fallbackValue);
						}
						actual.onComplete();
						//optionally still transmit the request to upstream + cancel it
						// to indicate eg. stress tests that we went down this code path
						if (this.trackRequestWhenFallbackDeferred) {
							s.request(n);
							s.cancel();
						}
					}
					return;
				}

				long u = Operators.addCap(r, n);
				if (REQUESTED.compareAndSet(this, r, u)) {
					s.request(n);
					return;
				}
			}
		}

		@Override
		public void onNext(T t) {
			producedOne();
			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (this.requested != STATE_CANCELLED) {
				REQUESTED.set(this, STATE_TERMINATED);
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			boolean shouldResume = this.resumableErrorPredicate == null || this.resumableErrorPredicate.test(t);
			if (!shouldResume) {
				//it is always fine to propagate errors even when there is no pending request
				if (this.requested != STATE_CANCELLED) {
					REQUESTED.set(this, STATE_TERMINATED);
					actual.onError(t);
				}
				return;
			}

			if (this.fallbackValue == null) {
				//it is always fine to send onComplete() even when there is no pending request
				if (this.requested != STATE_CANCELLED) {
					REQUESTED.set(this, STATE_TERMINATED);
					actual.onComplete();
				}
				return;
			}

//			in case of a fallback value, we need to make sure we don't send onNext(fallback) if there is no request
			long r = this.requested;
			if (r > 0) {
				//upstream won't produce anymore so we're confident the fallback value won't compete with an onNext for demand
				REQUESTED.set(this, STATE_TERMINATED);
				actual.onNext(this.fallbackValue);
				actual.onComplete();
				return;
			}

			//now the interesting case: r == 0 means we MUST wait for the next request before delivering the fallback value
			//upstream still won't produce, so the requested won't further diminish via producedOne()
			//we try to turn that 0 into a marker for pending fallback but if we fail we might want to propagate fallback ourselves
			if (!REQUESTED.compareAndSet(this, 0, STATE_PENDING_FALLBACK)) {
				//re-check the request:
				r = this.requested;
				if (r > 0) {
					REQUESTED.set(this, STATE_TERMINATED);
					actual.onNext(this.fallbackValue);
					actual.onComplete();
				}
				//at this point we failed to swap to PENDING_FALLBACK but there is no measurable request, which MUST mean we were cancelled
			} // else next request will take care of delivering
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PARENT) return s;

			long r = this.requested;
			if (key == Attr.CANCELLED) return r == STATE_CANCELLED;
			if (key == Attr.TERMINATED) return r == STATE_TERMINATED;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return r > 0 ? r : 0;

			return InnerOperator.super.scanUnsafe(key);
		}
	}
}

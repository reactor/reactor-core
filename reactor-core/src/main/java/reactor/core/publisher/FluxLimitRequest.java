/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 * @author David Karnok
 */
final class FluxLimitRequest<T> extends InternalFluxOperator<T, T> {

	final long cap;

	FluxLimitRequest(Flux<T> flux, long cap) {
		super(flux);
		if (cap < 0) {
			throw new IllegalArgumentException("cap >= 0 required but it was " + cap);
		}
		this.cap = cap;
	}

	@Override
	@Nullable
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (this.cap == 0) {
			Operators.complete(actual);
			return null;
		}
		return new FluxLimitRequestSubscriber<>(actual, this.cap);
	}

	@Override
	public int getPrefetch() {
		return 0;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return cap;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		//FluxOperator defines PREFETCH and PARENT
		return super.scanUnsafe(key);
	}

	static class FluxLimitRequestSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		Subscription parent;
		long toProduce;
		boolean done;

		volatile long requestRemaining;
		static final AtomicLongFieldUpdater<FluxLimitRequestSubscriber> REQUEST_REMAINING =
				AtomicLongFieldUpdater.newUpdater(FluxLimitRequestSubscriber.class, "requestRemaining");


		FluxLimitRequestSubscriber(CoreSubscriber<? super T> actual, long cap) {
			this.actual = actual;
			this.toProduce = cap;
			this.requestRemaining = cap;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			long r = toProduce;
			if (r > 0L) {
				toProduce = --r;
				actual.onNext(t);

				if (r == 0) {
					done = true;
					parent.cancel();
					actual.onComplete();
				}
			}
		}

		@Override
		public void onError(Throwable throwable) {
			if (done) {
				Operators.onErrorDropped(throwable, currentContext());
				return;
			}
			done = true;
			actual.onError(throwable);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.parent, s)) {
				parent = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void request(long l) {
			for (;;) {
				long r = requestRemaining;
				long newRequest;
				if (r <= l) {
					newRequest = r;
				} else {
					newRequest = l;
				}
				long u = r - newRequest;
				if (REQUEST_REMAINING.compareAndSet(this, r, u)) {
					if (newRequest != 0) {
						parent.request(newRequest);
					}
					break;
				}
			}
		}

		@Override
		public void cancel() {
			parent.cancel();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			//InnerOperator defines ACTUAL
			return InnerOperator.super.scanUnsafe(key);
		}
	}
}

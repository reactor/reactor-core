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

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class FluxDematerialize<T> extends InternalFluxOperator<Signal<T>, T> {

	FluxDematerialize(Flux<Signal<T>> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super Signal<T>> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new DematerializeSubscriber<>(actual, false);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class DematerializeSubscriber<T> implements InnerOperator<Signal<T>, T> {

		final CoreSubscriber<? super T> actual;
		final boolean                   completeAfterOnNext;

		Subscription s;

		boolean done;

		volatile boolean cancelled;

		DematerializeSubscriber(CoreSubscriber<? super T> subscriber, boolean completeAfterOnNext) {
			this.actual = subscriber;
			this.completeAfterOnNext = completeAfterOnNext;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(Signal<T> t) {
			if (done) {
				//TODO interpret the Signal and drop differently?
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			if (t.isOnComplete()) {
				s.cancel();
				onComplete();
			}
			else if (t.isOnError()) {
				s.cancel();
				onError(t.getThrowable());
			}
			else if (t.isOnNext()) {
				actual.onNext(t.get());
				if (completeAfterOnNext) {
					onComplete();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			actual.onError(t);
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
		public void request(long n) {
			if (Operators.validate(n)) {
				s.request(n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				s.cancel();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}
	}
}

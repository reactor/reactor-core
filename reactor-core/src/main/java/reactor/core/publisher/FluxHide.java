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
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Hides the identities of the upstream Publisher object and its Subscription
 * as well. 
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxHide<T> extends InternalFluxOperator<T, T> {

	FluxHide(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new HideSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
	    if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
	    return super.scanUnsafe(key);
	}

	static final class HideSubscriber<T> implements InnerOperator<T, T> {
		final CoreSubscriber<? super T> actual;

		Subscription s;
		
		HideSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class SuppressFuseableSubscriber<T>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;

		Subscription s;

		SuppressFuseableSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;

		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			return null;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}
}

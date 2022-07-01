/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import reactor.util.annotation.Nullable;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 *
 */
final class MonoCount<T> extends MonoFromFluxOperator<T, Long> implements Fuseable {

	MonoCount(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Long> actual) {
		return new CountSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class CountSubscriber<T> implements InnerOperator<T, Long>,
	                                                 Fuseable, //for constants only
	                                                 QueueSubscription<Long> {

		final CoreSubscriber<? super Long> actual;

		long counter;

		Subscription s;

		CountSubscriber(CoreSubscriber<? super Long> actual) {
			this.actual = actual;
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public CoreSubscriber<? super Long> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PREFETCH) return 0;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void request(long n) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			counter++;
		}

		@Override
		public void onError(Throwable t) {
			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			this.actual.onNext(counter);
			this.actual.onComplete();
		}

		@Override
		public Long poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}
	}
}

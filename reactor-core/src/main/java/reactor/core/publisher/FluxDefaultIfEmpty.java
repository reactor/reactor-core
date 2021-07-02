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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Emits a scalar value if the source sequence turns out to be empty.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDefaultIfEmpty<T> extends InternalFluxOperator<T, T> {

	final T value;

	FluxDefaultIfEmpty(Flux<? extends T> source, T value) {
		super(source);
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new DefaultIfEmptySubscriber<>(actual, value);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class DefaultIfEmptySubscriber<T>
			extends Operators.MonoSubscriber<T, T> {

		Subscription s;

		boolean hasValue;

		DefaultIfEmptySubscriber(CoreSubscriber<? super T> actual, T value) {
			super(actual);
			//noinspection deprecation
			this.value = value; //we write once, setValue() is NO-OP
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			super.request(n);
			s.request(n);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
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
			if (!hasValue) {
				hasValue = true;
			}

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (hasValue) {
				actual.onComplete();
			} else {
				complete(this.value);
			}
		}

		@Override
		public void setValue(T value) {
			// value is constant. writes from the base class are redundant, and the constant
			// would always be visible in cancel(), so it will safely be discarded.
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE; // prevent fusion because of the upstream
		}
	}
}

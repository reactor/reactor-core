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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.CoreSubscriber;
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
			extends Operators.BaseFluxToMonoOperator<T, T> {

		boolean done;

		boolean hasValue;

		volatile T fallbackValue;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultIfEmptySubscriber, Object> FALLBACK_VALUE =
				AtomicReferenceFieldUpdater.newUpdater(DefaultIfEmptySubscriber.class, Object.class, "fallbackValue");

		DefaultIfEmptySubscriber(CoreSubscriber<? super T> actual, T fallbackValue) {
			super(actual);
			FALLBACK_VALUE.lazySet(this, fallbackValue);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (!hasRequest) {
				hasRequest = true;

				final int state = this.state;

				if (state != 1 && STATE.compareAndSet(this, state, state | 1)) {
					if (state > 1) {
						final T fallbackValue = this.fallbackValue;
						if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this,
								fallbackValue,
								null)) {
							// completed before request means source was empty
							actual.onNext(fallbackValue);
							actual.onComplete();
 						}
						return;
					}
				}
			}

			s.request(n);
		}

		@Override
		public void cancel() {
			super.cancel();

			final T fallbackValue = this.fallbackValue;
			if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
				Operators.onDiscard(fallbackValue, actual.currentContext());
			}
		}

		@Override
		public void onNext(T t) {
			if (!hasValue) {
				hasValue = true;

				final T fallbackValue = this.fallbackValue;
				if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
					Operators.onDiscard(fallbackValue, actual.currentContext());
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;

			if (!hasValue) {
				completePossiblyEmpty();

				return;
			}

			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				return;
			}

			done = true;
			if (!hasValue) {
				final T fallbackValue = this.fallbackValue;
				if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
					Operators.onDiscard(t, actual.currentContext());
				}
			}

			actual.onError(t);
		}

		@Override
		T accumulatedValue() {
			final T fallbackValue = this.fallbackValue;
			if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
				return fallbackValue;
			}
			return null;
		}
	}
}

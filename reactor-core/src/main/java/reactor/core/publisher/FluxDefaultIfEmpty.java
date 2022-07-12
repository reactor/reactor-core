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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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
			implements InnerOperator<T, T>,
			           Fuseable, //for constants only
			           Fuseable.QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;

		Subscription s;

		boolean hasRequest;

		boolean hasValue;

		volatile T fallbackValue;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultIfEmptySubscriber, Object> FALLBACK_VALUE =
				AtomicReferenceFieldUpdater.newUpdater(DefaultIfEmptySubscriber.class, Object.class, "fallbackValue");

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DefaultIfEmptySubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(DefaultIfEmptySubscriber.class, "state");

		DefaultIfEmptySubscriber(CoreSubscriber<? super T> actual, T fallbackValue) {
			this.actual = actual;
			FALLBACK_VALUE.lazySet(this, fallbackValue);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
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
			s.cancel();
			final T fallbackValue = this.fallbackValue;
			if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
				Operators.onDiscard(fallbackValue, actual.currentContext());
			}
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

				final T fallbackValue = this.fallbackValue;
				if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
					Operators.onDiscard(fallbackValue, actual.currentContext());
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (!hasValue) {
				if (hasRequest) {
					final T fallbackValue = this.fallbackValue;
					if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this,
							fallbackValue,
							null)) {
						actual.onNext(fallbackValue);
						actual.onComplete();
					}
					return;
				}

				final int state = this.state;
				if (state == 0 && STATE.compareAndSet(this, 0, 2)) {
					return;
				}

				final T fallbackValue = this.fallbackValue;
				if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this,
						fallbackValue,
						null)) {
					actual.onNext(fallbackValue);
					actual.onComplete();
				}

				return;
			}

			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (!hasValue) {
				final T fallbackValue = this.fallbackValue;
				if (fallbackValue != null && FALLBACK_VALUE.compareAndSet(this, fallbackValue, null)) {
					Operators.onDiscard(t, actual.currentContext());
				}
			}

			actual.onError(t);
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE; // prevent fusion because of the upstream
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}
	}
}

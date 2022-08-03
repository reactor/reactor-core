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

import java.util.NoSuchElementException;
import java.util.Objects;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Take the very last value from a Publisher source and and emit that one.
 *
 * @param <T> the value type
 */
final class MonoTakeLastOne<T> extends MonoFromFluxOperator<T, T>
		implements Fuseable {

	final T defaultValue;

    MonoTakeLastOne(Flux<? extends T> source) {
        super(source);
	    this.defaultValue = null;
    }

	MonoTakeLastOne(Flux<? extends T> source, T defaultValue) {
		super(source);
		this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new TakeLastOneSubscriber<>(actual, defaultValue, true);
	}

	@Override
	public Object scanUnsafe(Attr key) {
    	if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class TakeLastOneSubscriber<T>
			extends Operators.BaseFluxToMonoOperator<T, T>  {

		static final Object CANCELLED = new Object();

		final boolean mustEmit;

		T value;

		boolean done;

		TakeLastOneSubscriber(CoreSubscriber<? super T> actual,
				@Nullable T defaultValue,
				boolean mustEmit) {
			super(actual);
			this.value = defaultValue;
			this.mustEmit = mustEmit;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done && value == null;
			if (key == Attr.CANCELLED) return value == CANCELLED;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			T old = this.value;
			if (old == CANCELLED) {
				// cancelled
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			synchronized (this) {
				old = this.value;
				if (old != CANCELLED) {
					this.value = t;
				}
			}

			if (old == CANCELLED) {
				// cancelled
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			Operators.onDiscard(old, actual.currentContext()); //FIXME cache context
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			this.done = true;


			final T v;
			synchronized (this) {
				v = this.value;
				this.value = null;
			}

			if (v == CANCELLED) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			if (v != null) {
				Operators.onDiscard(v, actual.currentContext());
			}

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}
			this.done = true;

			final T v = this.value;

			if (v == CANCELLED) {
				return;
			}

			if (v == null) {
				if (mustEmit) {
					actual.onError(Operators.onOperatorError(new NoSuchElementException(
							"Flux#last() didn't observe any " + "onNext signal"),
							actual.currentContext()));
				}
				else {
					actual.onComplete();
				}
				return;
			}

			completePossiblyEmpty();
		}

		@Override
		public void cancel() {
			s.cancel();

			final T v;
			synchronized (this) {
				v = this.value;
				//noinspection unchecked
				this.value = (T) CANCELLED;
			}

			if (v != null) {
				Operators.onDiscard(v, actual.currentContext());
			}
		}

		@Override
		T accumulatedValue() {
			final T v;
			synchronized (this) {
				v = this.value;
				this.value = null;
			}

			if (v == CANCELLED) {
				return null;
			}

			return v;
		}
	}
}

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

import java.util.NoSuchElementException;
import java.util.Objects;

import org.reactivestreams.Subscription;
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
			extends Operators.MonoSubscriber<T, T>  {

		final boolean mustEmit;
		final T       defaultValue;
		Subscription s;

		TakeLastOneSubscriber(CoreSubscriber<? super T> actual,
				@Nullable T defaultValue,
				boolean mustEmit) {
			super(actual);
			this.defaultValue = defaultValue;
			this.mustEmit = mustEmit;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}

		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			T old = this.value;
			setValue(t);
			Operators.onDiscard(old, actual.currentContext()); //FIXME cache context
		}

		@Override
		public void onComplete() {
			T v = this.value;
			if (v == null) {
				if (mustEmit) {
					if(defaultValue != null){
						complete(defaultValue);
					}
					else {
						actual.onError(Operators.onOperatorError(new NoSuchElementException(
								"Flux#last() didn't observe any " + "onNext signal"),
								actual.currentContext()));
					}
				}
				else {
					actual.onComplete();
				}
				return;
			}
			complete(v);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

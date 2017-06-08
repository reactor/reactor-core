/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Take the very last value from a Publisher source and and emit that one.
 *
 * @param <T> the value type
 */
final class MonoTakeLastOne<T> extends MonoOperator<T, T> implements Fuseable {

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
    public void subscribe(Subscriber<? super T> s, Context ctx) {
        source.subscribe(new TakeLastOneSubscriber<>(s, defaultValue, true), ctx);
    }

	static final class TakeLastOneSubscriber<T>
			extends Operators.MonoSubscriber<T, T>  {

		final boolean mustEmit;
		final T       defaultValue;
		Subscription s;

		TakeLastOneSubscriber(Subscriber<? super T> actual,
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
			if (key == ScannableAttr.PARENT) return s;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			value = t;
		}

		@Override
		public void onComplete() {
			T v = value;
			if (v == null) {
				if (mustEmit) {
					if(defaultValue != null){
						complete(defaultValue);
					}
					else {
						actual.onError(Operators.onOperatorError(new NoSuchElementException(
								"Flux#last() didn't observe any " + "onNext signal")));
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

		@Override
		public void setValue(T value) {
			// value is always in a field
		}
	}
}

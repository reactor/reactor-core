/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Receiver;

/**
 * Take the very last value from a Publisher source and and emit that one.
 *
 * @param <T> the value type
 */
final class MonoTakeLastOne<T> extends MonoSource<T, T> implements Fuseable {

    public MonoTakeLastOne(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new TakeLastOneSubscriber<>(s, true));
    }

	static final class TakeLastOneSubscriber<T>
			extends Operators.MonoSubscriber<T, T>
			implements Receiver {

		final boolean mustEmit;
		Subscription s;

		public TakeLastOneSubscriber(Subscriber<? super T> actual, boolean mustEmit) {
			super(actual);
			this.mustEmit = mustEmit;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}

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
					subscriber.onError(Operators.onOperatorError(new NoSuchElementException(
							"Flux#last() didn't observe any " + "onNext signal")));
				}
				else {
					subscriber.onComplete();
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

		@Override
		public Object upstream() {
			return s;
		}
	}
}

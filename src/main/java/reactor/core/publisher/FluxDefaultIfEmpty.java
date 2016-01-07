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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.BackpressureUtils;

/**
 * Emits a scalar value if the source sequence turns out to be empty.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxDefaultIfEmpty<T> extends Flux.FluxBarrier<T, T> {

	final T value;

	public FluxDefaultIfEmpty(Publisher<? extends T> source, T value) {
		super(source);
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new StreamDefaultIfEmptySubscriber<>(s, value));
	}

	static final class StreamDefaultIfEmptySubscriber<T>
			extends SubscriberDeferredScalar<T, T>
	implements Upstream{

		final T value;

		Subscription s;

		boolean hasValue;

		public StreamDefaultIfEmptySubscriber(Subscriber<? super T> actual, T value) {
			super(actual);
			this.value = value;
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
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!hasValue) {
				hasValue = true;
			}

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (hasValue) {
				subscriber.onComplete();
			} else {
				set(value);
			}
		}

		@Override
		public T get() {
			return value;
		}

		@Override
		public void setValue(T value) {
			// value is constant
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public Object delegateInput() {
			return value;
		}
	}
}

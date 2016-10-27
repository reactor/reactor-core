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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Producer;
import reactor.core.Receiver;

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoIgnoreThen<T> extends MonoSource<T, T> {

	public MonoIgnoreThen(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new IgnoreElementsSubscriber<>(s));
	}

	static final class IgnoreElementsSubscriber<T> implements Subscriber<T>, Producer, Subscription,
	                                                          Receiver {
		final Subscriber<? super T> actual;

		Subscription s;

		public IgnoreElementsSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
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
		public void onNext(T t) {
			// deliberately ignored
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
		public Object downstream() {
			return actual;
		}

		@Override
		public void request(long n) {
			// requests Long.MAX_VALUE anyway
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}

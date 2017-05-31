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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 *
 */
final class MonoCount<T> extends MonoSource<T, Long> implements Fuseable {

	MonoCount(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Long> s) {
		source.subscribe(new CountSubscriber<>(s));
	}

	static final class CountSubscriber<T> extends Operators.MonoSubscriber<T, Long>  {

		long counter;

		Subscription s;

		CountSubscriber(Subscriber<? super Long> actual) {
			super(actual);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return super.scanUnsafe(key);
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

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			counter++;
		}

		@Override
		public void onComplete() {
			complete(counter);
		}

	}
}

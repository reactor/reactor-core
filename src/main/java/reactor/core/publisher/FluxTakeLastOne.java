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
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.subscriber.SubscriptionHelper;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxTakeLastOne<T> extends FluxSource<T, T> implements Fuseable {

	public FluxTakeLastOne(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new TakeLastOneSubscriber<>(s));
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	static final class TakeLastOneSubscriber<T>
			extends OperatorHelper.DeferredScalarSubscriber<T, T>
			implements Receiver {

		Subscription s;

		public TakeLastOneSubscriber(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.validate(this.s, s)) {
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
				subscriber.onComplete();
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

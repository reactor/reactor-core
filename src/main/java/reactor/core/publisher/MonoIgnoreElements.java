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

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoIgnoreElements<T> extends reactor.Mono.MonoBarrier<T, T> {

	public MonoIgnoreElements(Publisher<? extends T> source) {
		super(source);
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new MonoIgnoreElementsSubscriber<>(s));
	}
	
	static final class MonoIgnoreElementsSubscriber<T> implements Subscriber<T>, Downstream {
		final Subscriber<? super T> actual;
		
		public MonoIgnoreElementsSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			actual.onSubscribe(s);
			s.request(Long.MAX_VALUE);
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
	}
}

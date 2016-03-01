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
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
final class FluxElapsed<T> extends FluxSource<T, Tuple2<Long, T>> {

	public FluxElapsed(Publisher<T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Tuple2<Long, T>> subscriber) {
		source.subscribe(new ElapsedAction<>(subscriber));
	}

	static final class ElapsedAction<T> extends SubscriberBarrier<T, Tuple2<Long, T>> {

		private long lastTime;

		public ElapsedAction(Subscriber<? super Tuple2<Long, T>> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			lastTime = System.currentTimeMillis();
			subscriber.onSubscribe(this);
		}

		@Override
		protected void doNext(T ev) {
			long previousTime = lastTime;
			lastTime = System.currentTimeMillis();

			subscriber.onNext(Tuple.of(lastTime - previousTime, ev));
		}
	}
}

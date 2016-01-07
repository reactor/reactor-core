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
import reactor.Mono;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.ReactiveState;

/**
 * Ignore onNext signals and therefore only pass request, cancel upstream and complete, error downstream
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public final class MonoIgnoreElements<IN> extends Mono<Void> implements ReactiveState.Upstream {

	private final Publisher<IN> source;

	public MonoIgnoreElements(Publisher<IN> source) {
		this.source = source;
	}

	@Override
	public Object upstream() {
		return source;
	}

	@Override
	public void subscribe(Subscriber<? super Void> subscriber) {
		source.subscribe(new CompletableBarrier<>(subscriber));
	}

	private static class CompletableBarrier<IN> extends SubscriberBarrier<IN, Void> {

		public CompletableBarrier(Subscriber<? super Void> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(IN in) {
		}

	}

}

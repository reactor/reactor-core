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
import reactor.core.subscriber.SubscriberBarrier;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
final class FluxDematerialize<T> extends FluxSource<Signal<T>, T> {

	public FluxDematerialize(Publisher<Signal<T>> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		source.subscribe(new DematerializeAction<>(subscriber));
	}

	static final class DematerializeAction<T> extends SubscriberBarrier<Signal<T>, T> {

		public DematerializeAction(Subscriber<? super T> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(Signal<T> ev) {
			if(!ev.isOnSubscribe()){
				if(ev.isOnNext()){
					subscriber.onNext(ev.get());
				}else if(ev.isOnComplete()){
					cancel();
					subscriber.onComplete();
				}else{
					cancel();
					subscriber.onError(ev.getThrowable());
				}
			}
		}
	}
}

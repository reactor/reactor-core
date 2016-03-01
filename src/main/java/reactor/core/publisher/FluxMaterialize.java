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
final class FluxMaterialize<T> extends FluxSource<T, Signal<T>> {

	public FluxMaterialize(Publisher<T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Signal<T>> subscriber) {
		source.subscribe(new MaterializeAction<>(subscriber));
	}

	final static class MaterializeAction<T> extends SubscriberBarrier<T, Signal<T>> {

		public MaterializeAction(Subscriber<? super Signal<T>> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(T ev) {
			subscriber.onNext(Signal.next(ev));
		}

		@Override
		protected void doError(Throwable ev) {
			subscriber.onNext(Signal.<T>error(ev));
			subscriber.onComplete();
		}

		@Override
		protected void doComplete() {
			subscriber.onNext(Signal.<T>complete());
			subscriber.onComplete();
		}
	}
}

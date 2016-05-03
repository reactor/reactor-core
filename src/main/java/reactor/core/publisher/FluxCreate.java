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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.util.EmptySubscription;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
final class FluxCreate<T> extends Flux<T> implements Introspectable {

	final Consumer<? super SignalEmitter<T>> yield;

	public FluxCreate(Consumer<? super SignalEmitter<T>> yield) {
		this.yield = yield;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		try {
			SignalEmitter<T> session = new RequestSignalEmitter<>(yield, subscriber);
			session.start();

		}
		catch (Throwable throwable) {
			EmptySubscription.error(subscriber, throwable);
		}
	}

	static final class RequestSignalEmitter<T> extends SignalEmitter<T> {

		final Consumer<? super SignalEmitter<T>> yield;

		@SuppressWarnings("unused")
		private volatile int running;

		@SuppressWarnings("rawtypes")
        private final static AtomicIntegerFieldUpdater<RequestSignalEmitter> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(RequestSignalEmitter.class, "running");

		public RequestSignalEmitter(Consumer<? super SignalEmitter<T>> yield, Subscriber<? super T> actual) {
			super(actual, false);
			this.yield = yield;
		}

		@Override
		public void request(long n) {
			if (isCancelled()) {
			    return;
			}
            super.request(n);
			if(RUNNING.getAndIncrement(this) == 0L){
				int missed = 1;

				for(;;) {
					yield.accept(this);

					missed = RUNNING.addAndGet(this, -missed);
					if(missed == 0) {
						break;
					}
				}
			}
		}
	}
}

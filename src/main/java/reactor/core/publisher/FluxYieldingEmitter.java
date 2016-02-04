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

import org.reactivestreams.Subscriber;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.util.EmptySubscription;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
final class FluxYieldingEmitter<T> extends Flux<T> implements Introspectable {

	final Consumer<? super SignalEmitter<T>> onSubscribe;

	public FluxYieldingEmitter(Consumer<? super SignalEmitter<T>> onSubscribe) {
		this.onSubscribe = onSubscribe;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		try {
			SignalEmitter<T> session = new YieldingSignalEmitter<>(onSubscribe, subscriber);
			session.start();

		}
		catch (FluxGenerate.PrematureCompleteException pce) {
			subscriber.onSubscribe(EmptySubscription.INSTANCE);
			subscriber.onComplete();
		}
		catch (Throwable throwable) {
			EmptySubscription.error(subscriber, throwable);
		}
	}

	private static class YieldingSignalEmitter<T> extends SignalEmitter<T> {

		final Consumer<? super SignalEmitter<T>> onSubscribe;

		@SuppressWarnings("unused")
		private volatile int running = 0;

		private final static AtomicIntegerFieldUpdater<YieldingSignalEmitter> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(YieldingSignalEmitter.class, "running");

		public YieldingSignalEmitter(Consumer<? super SignalEmitter<T>> onSubscribe, Subscriber<? super T> actual) {
			super(actual, false);
			this.onSubscribe = onSubscribe;
		}

		@Override
		public void request(long n) {
			super.request(n);
			if (RUNNING.getAndIncrement(this) == 0) {
				int missed = 1;

				onSubscribe.accept(this);

				for (; ; ) {
					missed = RUNNING.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}

			}
		}
	}
}

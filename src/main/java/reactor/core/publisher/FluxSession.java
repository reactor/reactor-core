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
import reactor.Flux;
import reactor.core.subscription.EmptySubscription;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class FluxSession<T> extends Flux<T> implements ReactiveState.Factory {

	final Consumer<? super ReactiveSession<T>> onSubscribe;

	public FluxSession(Consumer<? super ReactiveSession<T>> onSubscribe) {
		this.onSubscribe = onSubscribe;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		try {
			ReactiveSession<T> session = new YieldingReactiveSession<>(onSubscribe, subscriber);
			session.start();

		}
		catch (FluxFactory.PrematureCompleteException pce) {
			subscriber.onSubscribe(EmptySubscription.INSTANCE);
			subscriber.onComplete();
		}
		catch (Throwable throwable) {
			EmptySubscription.error(subscriber, throwable);
		}
	}

	private static class YieldingReactiveSession<T> extends ReactiveSession<T> {

		final Consumer<? super ReactiveSession<T>> onSubscribe;

		@SuppressWarnings("unused")
		private volatile int running = 0;

		private final static AtomicIntegerFieldUpdater<YieldingReactiveSession> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(YieldingReactiveSession.class, "running");

		public YieldingReactiveSession(Consumer<? super ReactiveSession<T>> onSubscribe, Subscriber<? super T> actual) {
			super(actual);
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

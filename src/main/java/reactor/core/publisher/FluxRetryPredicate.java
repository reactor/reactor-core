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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.MultiSubscriptionSubscriber;
import reactor.core.util.Exceptions;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRetryPredicate<T> extends FluxSource<T, T> {

	final Predicate<Throwable> predicate;

	public FluxRetryPredicate(Publisher<? extends T> source, Predicate<Throwable> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		RetryPredicateSubscriber<T> parent = new RetryPredicateSubscriber<>(source, s, predicate);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.resubscribe();
		}
	}

	static final class RetryPredicateSubscriber<T>
			extends MultiSubscriptionSubscriber<T, T> {

		final Publisher<? extends T> source;

		final Predicate<Throwable> predicate;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RetryPredicateSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryPredicateSubscriber.class, "wip");

		long produced;

		public RetryPredicateSubscriber(Publisher<? extends T> source, 
				Subscriber<? super T> actual, Predicate<Throwable> predicate) {
			super(actual);
			this.source = source;
			this.predicate = predicate;
		}

		@Override
		public void onNext(T t) {
			produced++;

			subscriber.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			boolean b;
			
			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				Throwable _t = Exceptions.unwrap(e);
				_t.addSuppressed(t);
				subscriber.onError(_t);
				return;
			}
			
			if (b) {
				resubscribe();
			} else {
				subscriber.onError(t);
			}
		}
		
		@Override
		public void onComplete() {
			
			subscriber.onComplete();
		}

		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (isCancelled()) {
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
			}
		}
	}
}

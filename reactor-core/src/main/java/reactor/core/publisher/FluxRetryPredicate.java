/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRetryPredicate<T> extends InternalFluxOperator<T, T> {

	final Predicate<? super Throwable> predicate;

	FluxRetryPredicate(Flux<? extends T> source, Predicate<? super Throwable> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {

		RetryPredicateSubscriber<T> parent = new RetryPredicateSubscriber<>(source,
				actual,
				predicate);

		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.resubscribe();
		}
		return null;
	}

	static final class RetryPredicateSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final CorePublisher<? extends T> source;

		final Predicate<? super Throwable> predicate;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RetryPredicateSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryPredicateSubscriber.class, "wip");

		long produced;

		RetryPredicateSubscriber(CorePublisher<? extends T> source,
				CoreSubscriber<? super T> actual, Predicate<? super Throwable> predicate) {
			super(actual);
			this.source = source;
			this.predicate = predicate;
		}

		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			boolean b;
			
			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Throwable _t = Operators.onOperatorError(e, actual.currentContext());
				_t = Exceptions.addSuppressed(_t, t);
				actual.onError(_t);
				return;
			}
			
			if (b) {
				resubscribe();
			} else {
				actual.onError(t);
			}
		}
		
		@Override
		public void onComplete() {
			
			actual.onComplete();
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

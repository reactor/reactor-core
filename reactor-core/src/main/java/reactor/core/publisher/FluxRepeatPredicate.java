/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.function.BooleanSupplier;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRepeatPredicate<T> extends FluxOperator<T, T> {

	final BooleanSupplier predicate;

	FluxRepeatPredicate(Flux<? extends T> source, BooleanSupplier predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		RepeatPredicateSubscriber<T> parent = new RepeatPredicateSubscriber<>(source,
				s, predicate);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.resubscribe();
		}
	}

	static final class RepeatPredicateSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Publisher<? extends T> source;

		final BooleanSupplier predicate;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RepeatPredicateSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RepeatPredicateSubscriber.class, "wip");

		long produced;

		RepeatPredicateSubscriber(Publisher<? extends T> source,
				CoreSubscriber<? super T> actual, BooleanSupplier predicate) {
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
		public void onComplete() {
			boolean b;
			
			try {
				b = predicate.getAsBoolean();
			} catch (Throwable e) {
				actual.onError(Operators.onOperatorError(e));
				return;
			}
			
			if (b) {
				resubscribe();
			} else {
				actual.onComplete();
			}
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

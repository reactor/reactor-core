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
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;

/**
 * Aggregates the source items with an aggregator function and returns the last result.
 *
 * @param <T> the input and output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoReduce<T> extends MonoSource<T, T> implements Fuseable {

	final BiFunction<T, T, T> aggregator;

	MonoReduce(Publisher<? extends T> source, BiFunction<T, T, T> aggregator) {
		super(source);
		this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new ReduceSubscriber<>(s, aggregator));
	}

	static final class ReduceSubscriber<T> extends Operators.MonoSubscriber<T, T> {

		final BiFunction<T, T, T> aggregator;

		Subscription s;

		T result;

		boolean done;

		ReduceSubscriber(Subscriber<? super T> actual,
				BiFunction<T, T, T> aggregator) {
			super(actual);
			this.aggregator = aggregator;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			T r = result;
			if (r == null) {
				result = t;
			}
			else {
				try {
					r = Objects.requireNonNull(aggregator.apply(r, t),
							"The aggregator returned a null value");
				}
				catch (Throwable ex) {
					result = null;
					done = true;
					actual.onError(Operators.onOperatorError(s, ex, t));
					return;
				}

				result = r;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			result = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			T r = result;
			if (r != null) {
				complete(r);
			}
			else {
				actual.onComplete();
			}
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public boolean isStarted() {
			return s != null && !isTerminated();
		}
	}
}

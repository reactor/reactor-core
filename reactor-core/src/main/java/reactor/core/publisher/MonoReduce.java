/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Aggregates the source items with an aggregator function and returns the last result.
 *
 * @param <T> the input and output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoReduce<T> extends MonoFromFluxOperator<T, T>
		implements Fuseable {

	final BiFunction<T, T, T> aggregator;

	MonoReduce(Flux<? extends T> source, BiFunction<T, T, T> aggregator) {
		super(source);
		this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new ReduceSubscriber<>(actual, aggregator);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ReduceSubscriber<T> extends Operators.MonoSubscriber<T, T> {

		final BiFunction<T, T, T> aggregator;

		Subscription s;

		boolean done;

		ReduceSubscriber(CoreSubscriber<? super T> actual,
				BiFunction<T, T, T> aggregator) {
			super(actual);
			this.aggregator = aggregator;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
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
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			T r = this.value;
			if (r == null) {
				setValue(t);
			}
			else {
				try {
					r = Objects.requireNonNull(aggregator.apply(r, t),
							"The aggregator returned a null value");
				}
				catch (Throwable ex) {
					done = true;
					Context ctx = actual.currentContext();
					Operators.onDiscard(t, ctx);
					Operators.onDiscard(this.value, ctx);
					this.value = null;
					actual.onError(Operators.onOperatorError(s, ex, t,
							actual.currentContext()));
					return;
				}

				setValue(r);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			discard(this.value);
			this.value = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			T r = this.value;
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
	}
}

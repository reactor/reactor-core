/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

	static final class ReduceSubscriber<T>  implements InnerOperator<T, T>,
	                                                   Fuseable,
	                                                   QueueSubscription<T> {

		static final Object CANCELLED = new Object();

		final BiFunction<T, T, T> aggregator;
		final CoreSubscriber<? super T> actual;

		T aggregate;

		Subscription s;

		boolean done;

		ReduceSubscriber(CoreSubscriber<? super T> actual,
				BiFunction<T, T, T> aggregator) {
			this.actual = actual;
			this.aggregator = aggregator;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return !done && aggregate == CANCELLED;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			final T r = this.aggregate;
			if (r == CANCELLED) {
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			// initial scenario when aggregate has nothing in it
			if (r == null) {
				synchronized (this) {
					if (this.aggregate == null) {
						this.aggregate = t;
						return;
					}
				}

				Operators.onDiscard(t, actual.currentContext());
			}
			else {
				try {
					synchronized (this) {
						if (this.aggregate != CANCELLED) {
							this.aggregate = Objects.requireNonNull(aggregator.apply(r, t), "The aggregator returned a null value");
							return;
						}
					}
					Operators.onDiscard(t, actual.currentContext());
				}
				catch (Throwable ex) {
					done = true;
					Context ctx = actual.currentContext();
					synchronized (this) {
						this.aggregate = null;
					}
					Operators.onDiscard(t, ctx);
					Operators.onDiscard(r, ctx);
					actual.onError(Operators.onOperatorError(s, ex, t,
							actual.currentContext()));
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;

			final T r;
			synchronized (this) {
				r = this.aggregate;
				this.aggregate = null;
			}

			if (r == CANCELLED) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			if (r != null) {
				Operators.onDiscard(r, actual.currentContext());
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			final T r;
			synchronized (this) {
				r = this.aggregate;
				this.aggregate = null;
			}

			if (r == CANCELLED) {
				return;
			}

			if (r == null) {
				actual.onComplete();
			}
			else {
				actual.onNext(r);
				actual.onComplete();
			}
		}

		@Override
		public void cancel() {
			s.cancel();

			final T r;
			synchronized (this) {
				r = this.aggregate;
				//noinspection unchecked
				this.aggregate = (T) CANCELLED;
			}

			if (r == null || r == CANCELLED) {
				return;
			}

			Operators.onDiscard(r, actual.currentContext());
		}

		@Override
		public void request(long n) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}
	}
}

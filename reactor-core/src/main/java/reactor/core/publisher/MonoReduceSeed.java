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
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Aggregates the source values with the help of an accumulator
 * function and emits the final accumulated value.
 *
 * @param <T> the source value type
 * @param <R> the accumulated result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoReduceSeed<T, R> extends MonoFromFluxOperator<T, R>
		implements Fuseable {

	final Supplier<R> initialSupplier;

	final BiFunction<R, ? super T, R> accumulator;

	MonoReduceSeed(Flux<? extends T> source,
			Supplier<R> initialSupplier,
			BiFunction<R, ? super T, R> accumulator) {
		super(source);
		this.initialSupplier = Objects.requireNonNull(initialSupplier, "initialSupplier");
		this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		R initialValue = Objects.requireNonNull(initialSupplier.get(),
				"The initial value supplied is null");

		return new ReduceSeedSubscriber<>(actual, accumulator, initialValue);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ReduceSeedSubscriber<T, R> extends Operators.BaseFluxToMonoOperator<T, R>  {

		final BiFunction<R, ? super T, R> accumulator;

		R seed;

		boolean done;

		ReduceSeedSubscriber(CoreSubscriber<? super R> actual,
				BiFunction<R, ? super T, R> accumulator,
				R seed) {
			super(actual);
			this.accumulator = accumulator;
			this.seed = seed;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return !done && seed == null;

			return super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			s.cancel();

			final R seed;
			synchronized (this) {
				seed = this.seed;
				this.seed = null;
			}

			if (seed == null) {
				return;
			}

			Operators.onDiscard(seed, actual.currentContext());
		}

		@Override
		public void onNext(T t) {
			final R v;
			final R accumulated;

			try {
				synchronized (this) {
					v = this.seed;
					if (v != null) {
						accumulated = Objects.requireNonNull(accumulator.apply(v, t),
								"The accumulator returned a null value");
						this.seed = accumulated;
						return;
					}
				}

				// the actual seed is null, meaning cancelled, new state have to be
				// discarded as well
				Operators.onDiscard(t, actual.currentContext());
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(this.s, e, t, actual.currentContext()));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;

			final R seed;
			synchronized (this) {
				seed = this.seed;
				this.seed = null;
			}

			if (seed == null) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			Operators.onDiscard(seed, actual.currentContext());

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			completePossiblyEmpty();
		}

		@Override
		R accumulatedValue() {
			final R seed;
			synchronized (this) {
				seed = this.seed;
				this.seed = null;
			}
			return seed;
		}
	}
}

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
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
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

	static final class ReduceSeedSubscriber<T, R> extends Operators.MonoSubscriber<T, R>  {

		final BiFunction<R, ? super T, R> accumulator;

		Subscription s;

		boolean done;

		ReduceSeedSubscriber(CoreSubscriber<? super R> actual,
				BiFunction<R, ? super T, R> accumulator,
				R value) {
			super(actual);
			this.accumulator = accumulator;
			//noinspection deprecation
			this.value = value; //setValue is made NO-OP in order to ignore redundant writes in base class
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
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public void setValue(R value) {
			// value is updated directly in onNext. writes from the base class are redundant.
			// if cancel() happens before first reduction, the seed is visible from constructor and will be discarded.
			// if there was some accumulation in progress post cancel, onNext will take care of it.
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
			R v = this.value;
			R accumulated;

			if (v != null) { //value null when cancelled
				try {
					accumulated = Objects.requireNonNull(accumulator.apply(v, t),
							"The accumulator returned a null value");

				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(this, e, t, actual.currentContext()));
					return;
				}
				if (STATE.get(this) == CANCELLED) {
					discard(accumulated);
					this.value = null;
				}
				else {
					//noinspection deprecation
					this.value = accumulated; //setValue is made NO-OP in order to ignore redundant writes in base class
				}
			} else {
				Operators.onDiscard(t, actual.currentContext());
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

			complete(this.value);
			//we DON'T null out the value, complete will do that once there's been a request
		}
	}
}

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
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Aggregates the source values with the help of an accumulator function
 * and emits the intermediate results.
 * <p>
 * The accumulation works as follows:
 * <pre><code>
 * result[0] = initialValue;
 * result[1] = accumulator(result[0], source[0])
 * result[2] = accumulator(result[1], source[1])
 * result[3] = accumulator(result[2], source[2])
 * ...
 * </code></pre>
 *
 * @param <T> the source value type
 * @param <R> the aggregate type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxScanSeed<T, R> extends FluxSource<T, R> {

	final BiFunction<R, ? super T, R> accumulator;

	final Supplier<R> initialSupplier;

	FluxScanSeed(Flux<? extends T> source,
			Supplier<R> initialSupplier,
			BiFunction<R, ? super T, R> accumulator) {
		super(source);
		this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
		this.initialSupplier = Objects.requireNonNull(initialSupplier, "initialSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		R initialValue;

		try {
			initialValue = Objects.requireNonNull(initialSupplier.get(),
					"The initial value supplied is null");
		}
		catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}
		source.subscribe(new ScanSeedSubscriber<>(s, accumulator, initialValue));
	}

	static final class ScanSeedSubscriber<T, R> implements InnerOperator<T, R> {

		final Subscriber<? super R> actual;

		final BiFunction<R, ? super T, R> accumulator;

		Subscription s;

		R value;

		boolean done;

		volatile int state;
		long produced;

		ScanSeedSubscriber(Subscriber<? super R> actual,
				BiFunction<R, ? super T, R> accumulator,
				R initialValue) {
			this.actual = actual;
			this.accumulator = accumulator;
			this.value = initialValue;
		}

		@Override
		public Subscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			STATE.set(this, CANCELLED);
			s.cancel();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			sendSeed();

			R r = value;

			try {
				r = Objects.requireNonNull(accumulator.apply(r, t),
						"The accumulator returned a null value");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			serializedOnNext(r);
			value = r;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (STATE.get(this) == 0) {
					if (sendSeed()) {
						if (n == Long.MAX_VALUE) {
							s.request(n);
						}
						else {
							s.request(Math.max(0, n - 1));
						}
					} else {
						s.request(n);
					}
				}
				else {
					s.request(n);
				}
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) {
				return s;
			}
			if (key == BooleanAttr.TERMINATED) {
				return done;
			}
			if (key == IntAttr.BUFFERED) {
				return value != null ? 1 : 0;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		private boolean sendSeed() {
			if (STATE.compareAndSet(this, 0, SENT_SEED)) {
				serializedOnNext(value);
				return true;
			}
			return false;
		}

		// concurrent calls to request at the start could yield concurrent calls to
		// onNext. this will have low-to-no contention after the initial state has emitted
		private synchronized void serializedOnNext(R value) {
			actual.onNext(value);
		}

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScanSeedSubscriber> STATE     =
				AtomicIntegerFieldUpdater.newUpdater(ScanSeedSubscriber.class, "state");
		static final int                                           SENT_SEED = 1;
		static final int                                           CANCELLED = 2;

	}
}

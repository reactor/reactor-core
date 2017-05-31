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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static reactor.core.publisher.DrainUtils.COMPLETED_MASK;
import static reactor.core.publisher.DrainUtils.REQUESTED_MASK;

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

	static final class ScanSeedSubscriber<T, R>
			implements InnerOperator<T, R> {

		final Subscriber<? super R> actual;

		final BiFunction<R, ? super T, R> accumulator;

		Subscription s;

		R value;

		boolean done;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ScanSeedSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ScanSeedSubscriber.class, "requested");

		long produced;

		ScanSeedSubscriber(Subscriber<? super R> actual,
				BiFunction<R, ? super T, R> accumulator,
				R initialValue) {
			this.actual = actual;
			this.accumulator = accumulator;
			this.value = initialValue;
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
				Operators.onNextDropped(t);
				return;
			}

			R r = value;

			produced++;

			actual.onNext(r);

			try {
				r = Objects.requireNonNull(accumulator.apply(r, t),
						"The accumulator returned a null value");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			value = r;
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
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			R v = value;

			long p = produced;
			long r = requested;
			if (r != Long.MAX_VALUE && p != 0L) {
				r = REQUESTED.addAndGet(this, -p);
			}

			for (; ; ) {
				// if any request amount is still available, emit the value and complete
				if ((r & REQUESTED_MASK) != 0L) {
					actual.onNext(v);
					actual.onComplete();
					return;
				}
				// NO_REQUEST_NO_VALUE -> NO_REQUEST_HAS_VALUE
				if (REQUESTED.compareAndSet(this, 0, COMPLETED_MASK)) {
					return;
				}

				r = requested;
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				for (; ; ) {

					long r = requested;

					// NO_REQUEST_HAS_VALUE 
					if (r == COMPLETED_MASK) {
						// any positive request value will do here
						// transition to HAS_REQUEST_HAS_VALUE
						if (REQUESTED.compareAndSet(this,
								COMPLETED_MASK,
								COMPLETED_MASK | 1)) {
							actual.onNext(value);
							actual.onComplete();
						}
						return;
					}

					// HAS_REQUEST_HAS_VALUE
					if (r < 0L) {
						return;
					}

					// transition to HAS_REQUEST_NO_VALUE
					long u = Operators.addCap(r, n);
					if (REQUESTED.compareAndSet(this, r, u)) {
						s.request(n);
						return;
					}
				}
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == IntAttr.BUFFERED) return value != null ? 1 : 0;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super R> actual() {
			return actual;
		}

	}
}

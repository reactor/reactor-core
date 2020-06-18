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
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

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
final class FluxScanSeed<T, R> extends InternalFluxOperator<T, R> {

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
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		ScanSeedCoordinator<T, R> coordinator =
				new ScanSeedCoordinator<>(actual, source, accumulator, initialSupplier);

		actual.onSubscribe(coordinator);

		if (!coordinator.isCancelled()) {
			coordinator.onComplete();
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == RUN_STYLE) return SYNC;
		return super.scanUnsafe(key);
	}

	static final class ScanSeedCoordinator<T, R>
			extends Operators.MultiSubscriptionSubscriber<R, R> {

		final    Supplier<R>                 initialSupplier;
		final    Flux<? extends T>           source;
		final    BiFunction<R, ? super T, R> accumulator;
		volatile int                         wip;
		long produced;
		private ScanSeedSubscriber<T, R> seedSubscriber;

		ScanSeedCoordinator(CoreSubscriber<? super R> actual, Flux<? extends T> source,
				BiFunction<R, ? super T, R> accumulator,
				Supplier<R> initialSupplier) {
			super(actual);
			this.source = source;
			this.accumulator = accumulator;
			this.initialSupplier = initialSupplier;
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (isCancelled()) {
						return;
					}

					if (null != seedSubscriber && subscription == seedSubscriber) {
						actual.onComplete();
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					if (null == seedSubscriber) {
						R initialValue;

						try {
							initialValue = Objects.requireNonNull(initialSupplier.get(),
									"The initial value supplied is null");
						}
						catch (Throwable e) {
							onError(Operators.onOperatorError(e, actual.currentContext()));
							return;
						}

						onSubscribe(Operators.scalarSubscription(this, initialValue));
						seedSubscriber =
								new ScanSeedSubscriber<>(this, accumulator, initialValue);
					}
					else {
						source.subscribe(seedSubscriber);
					}

					if (isCancelled()) {
						return;
					}
				}
				while (WIP.decrementAndGet(this) != 0);
			}

		}

		@Override
		public void onNext(R r) {
			produced++;
			actual.onNext(r);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == RUN_STYLE) return SYNC;
			return super.scanUnsafe(key);
		}

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScanSeedCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ScanSeedCoordinator.class, "wip");
	}

	static final class ScanSeedSubscriber<T, R> implements InnerOperator<T, R> {

		final CoreSubscriber<? super R> actual;

		final BiFunction<R, ? super T, R> accumulator;

		Subscription s;

		R value;

		boolean done;

		ScanSeedSubscriber(CoreSubscriber<? super R> actual,
				BiFunction<R, ? super T, R> accumulator,
				R initialValue) {
			this.actual = actual;
			this.accumulator = accumulator;
			this.value = initialValue;
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			value = null;
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			value = null;
			actual.onError(t);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			R r = value;

			try {
				r = Objects.requireNonNull(accumulator.apply(r, t),
						"The accumulator returned a null value");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			actual.onNext(r);
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
			s.request(n);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == RUN_STYLE) {
			    return SYNC;
			}
			return InnerOperator.super.scanUnsafe(key);
		}
	}
}

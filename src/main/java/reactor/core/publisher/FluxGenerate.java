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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.*;

import org.reactivestreams.Subscriber;

import reactor.core.flow.Fuseable;
import reactor.core.flow.Fuseable.QueueSubscription;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.util.*;

/**
 * Generate signals one-by-one via a function callback.
 * <p>
 * <p>
 * The {@code stateSupplier} may return {@code null} but your {@code stateConsumer} should be prepared to
 * handle it.
 *
 * @param <T> the value type emitted
 * @param <S> the custom state per subscriber
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 2.5
 */
final class FluxGenerate<T, S> 
extends Flux<T> {

	final Callable<S> stateSupplier;

	final BiFunction<S, SignalEmitter<T>, S> generator;

	final Consumer<? super S> stateConsumer;

	public FluxGenerate(BiFunction<S, SignalEmitter<T>, S> generator) {
		this(() -> null, generator, s -> {
		});
	}

	public FluxGenerate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator) {
		this(stateSupplier, generator, s -> {
		});
	}

	public FluxGenerate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator,
							 Consumer<? super S> stateConsumer) {
		this.stateSupplier = Objects.requireNonNull(stateSupplier, "stateSupplier");
		this.generator = Objects.requireNonNull(generator, "generator");
		this.stateConsumer = Objects.requireNonNull(stateConsumer, "stateConsumer");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		S state;

		try {
			state = stateSupplier.call();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}
		s.onSubscribe(new GenerateSubscription<>(s, state, generator, stateConsumer));
	}

	static final class GenerateSubscription<T, S>
	  implements QueueSubscription<T>, SignalEmitter<T> {

		final Subscriber<? super T> actual;

		final BiFunction<S, SignalEmitter<T>, S> generator;

		final Consumer<? super S> stateConsumer;

		volatile boolean cancelled;

		S state;

		boolean terminate;

		boolean hasValue;
		
		boolean outputFused;
		
		T generatedValue;
		
		Throwable generatedError;

		volatile long requested;

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<GenerateSubscription> REQUESTED = 
			AtomicLongFieldUpdater.newUpdater(GenerateSubscription.class, "requested");

		public GenerateSubscription(Subscriber<? super T> actual, S state,
											 BiFunction<S, SignalEmitter<T>, S> generator, Consumer<? super
		  S> stateConsumer) {
			this.actual = actual;
			this.state = state;
			this.generator = generator;
			this.stateConsumer = stateConsumer;
		}

		@Override
		public void next(T t) {
			if (terminate) {
				Exceptions.onNextDropped(t);
				return;
			}
			if (hasValue) {
				fail(new IllegalStateException("More than one call to onNext"));
				return;
			}
			if (t == null) {
				fail(new NullPointerException("The generator produced a null value"));
				return;
			}
			hasValue = true;
			if (outputFused) {
				generatedValue = t;
			} else {
				actual.onNext(t);
			}
		}

		@Override
		public void fail(Throwable e) {
			if (terminate) {
				return;
			}
			terminate = true;
			if (outputFused) {
				generatedError = e;
			} else {
				actual.onError(e);
			}
		}

		@Override
		public void complete() {
			if (terminate) {
				return;
			}
			terminate = true;
			if (!outputFused) {
				actual.onComplete();
			}
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (BackpressureUtils.getAndAddCap(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					} else {
						slowPath(n);
					}
				}
			}
		}

		void fastPath() {
			S s = state;

			final BiFunction<S, SignalEmitter<T>, S> g = generator;

			for (; ; ) {

				if (cancelled) {
					cleanup(s);
					return;
				}

				try {
					s = g.apply(s, this);
				} catch (Throwable e) {
					cleanup(s);

					actual.onError(e);
					return;
				}
				if (terminate || cancelled) {
					cleanup(s);
					return;
				}
				if (!hasValue) {
					cleanup(s);

					actual.onError(new IllegalStateException("The generator didn't call any of the " +
					  "SignalEmitter method"));
					return;
				}

				hasValue = false;
			}
		}

		void slowPath(long n) {
			S s = state;

			long e = 0L;

			final BiFunction<S, SignalEmitter<T>, S> g = generator;

			for (; ; ) {
				while (e != n) {

					if (cancelled) {
						cleanup(s);
						return;
					}

					try {
						s = g.apply(s, this);
					} catch (Throwable ex) {
						cleanup(s);

						actual.onError(ex);
						return;
					}
					if (terminate || cancelled) {
						cleanup(s);
						return;
					}
					if (!hasValue) {
						cleanup(s);

						actual.onError(new IllegalStateException("The generator didn't call any of the " +
						  "SignalEmitter method"));
						return;
					}

					e++;
					hasValue = false;
				}

				n = requested;

				if (n == e) {
					state = s;
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0L) {
						return;
					}
				}
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				if (REQUESTED.getAndIncrement(this) == 0) {
					cleanup(state);
				}
			}
		}

		void cleanup(S s) {
			try {
				state = null;

				stateConsumer.accept(s);
			} catch (Throwable e) {
				Exceptions.onErrorDropped(e);
			}
		}
		
		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0 && (requestedMode & Fuseable.THREAD_BARRIER) == 0) {
				outputFused = true;
				return Fuseable.SYNC;
			}
			return Fuseable.NONE;
		}
		
		@Override
		public T poll() {
			if (terminate) {
				return null;
			}

			S s = state;
			
			try {
				s = generator.apply(s, this);
			} catch (final Throwable ex) {
				cleanup(s);
				throw ex;
			}
			
			Throwable e = generatedError;
			if (e != null) {
				cleanup(s);
				
				generatedError = null;
				throw Exceptions.bubble(e);
			}
			
			if (!hasValue) {
				cleanup(s);
				
				if (!terminate) {
					throw new IllegalStateException("The generator didn't call any of the " +
						"SignalEmitter method");
				}
				return null;
			}
			
			T v = generatedValue;
			generatedValue = null;
			hasValue = false;

			state = s;
			return v;
		}

		@Override
		public Throwable getError() {
			return generatedError;
		}

		@Override
		public boolean isEmpty() {
			return terminate;
		}
		
		@Override
		public int size() {
			return isEmpty() ? 0 : -1;
		}
		
		@Override
		public void clear() {
			generatedError = null;
			generatedValue = null;
		}
	}
}

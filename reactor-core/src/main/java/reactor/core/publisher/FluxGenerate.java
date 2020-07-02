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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Generate signals one-by-one via a function callback.
 * <p>
 * <p>
 * The {@code stateSupplier} may return {@code null} but your {@code stateConsumer} should be prepared to
 * handle it.
 *
 * @param <T> the value type emitted
 * @param <S> the custom state per subscriber
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxGenerate<T, S>
extends Flux<T> implements Fuseable, SourceProducer<T> {


	static final Callable EMPTY_CALLABLE = () -> null;

	final Callable<S> stateSupplier;

	final BiFunction<S, SynchronousSink<T>, S> generator;

	final Consumer<? super S> stateConsumer;

	@SuppressWarnings("unchecked")
	FluxGenerate(Consumer<SynchronousSink<T>> generator) {
		this(EMPTY_CALLABLE, (state,sink) -> {
			generator.accept(sink);
			return null;
		});
	}

	FluxGenerate(Callable<S> stateSupplier, BiFunction<S, SynchronousSink<T>, S> generator) {
		this(stateSupplier, generator, s -> {
		});
	}

	FluxGenerate(Callable<S> stateSupplier, BiFunction<S, SynchronousSink<T>, S> generator,
							 Consumer<? super S> stateConsumer) {
		this.stateSupplier = Objects.requireNonNull(stateSupplier, "stateSupplier");
		this.generator = Objects.requireNonNull(generator, "generator");
		this.stateConsumer = Objects.requireNonNull(stateConsumer, "stateConsumer");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		S state;

		try {
			state = stateSupplier.call();
		} catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}
		actual.onSubscribe(new GenerateSubscription<>(actual, state, generator, stateConsumer));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class GenerateSubscription<T, S>
	  implements QueueSubscription<T>, InnerProducer<T>, SynchronousSink<T> {

		final CoreSubscriber<? super T> actual;

		final BiFunction<S, SynchronousSink<T>, S> generator;

		final Consumer<? super S> stateConsumer;

		volatile boolean cancelled;

		S state;

		boolean terminate;

		boolean hasValue;
		
		boolean outputFused;
		
		T generatedValue;
		
		Throwable generatedError;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<GenerateSubscription> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(GenerateSubscription.class, "requested");

		GenerateSubscription(CoreSubscriber<? super T> actual, S state,
											 BiFunction<S, SynchronousSink<T>, S> generator, Consumer<? super
		  S> stateConsumer) {
			this.actual = actual;
			this.state = state;
			this.generator = generator;
			this.stateConsumer = stateConsumer;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return terminate;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return generatedError;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void next(T t) {
			if (terminate) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (hasValue) {
				error(new IllegalStateException("More than one call to onNext"));
				return;
			}
			//noinspection ConstantConditions
			if (t == null) {
				error(new NullPointerException("The generator produced a null value"));
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
		public void error(Throwable e) {
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
			if (Operators.validate(n)) {
				if (Operators.addCap(REQUESTED, this, n) == 0) {
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

			final BiFunction<S, SynchronousSink<T>, S> g = generator;

			for (; ; ) {

				if (cancelled) {
					cleanup(s);
					return;
				}

				try {
					s = g.apply(s, this);
				} catch (Throwable e) {
					cleanup(s);

					actual.onError(Operators.onOperatorError(e, actual.currentContext()));
					return;
				}
				if (terminate || cancelled) {
					cleanup(s);
					return;
				}
				if (!hasValue) {
					cleanup(s);

					actual.onError(new IllegalStateException("The generator didn't call any of the " +
					  "SynchronousSink method"));
					return;
				}

				hasValue = false;
			}
		}

		void slowPath(long n) {
			S s = state;

			long e = 0L;

			final BiFunction<S, SynchronousSink<T>, S> g = generator;

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
						  "SynchronousSink method"));
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
				Operators.onErrorDropped(e, actual.currentContext());
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
		@Nullable
		public T poll() {
			S s = state;

			if (terminate) {
				cleanup(s);

				Throwable e = generatedError;
				if (e != null) {

					generatedError = null;
					throw Exceptions.propagate(e);
				}

				return null;
			}

			
			try {
				s = generator.apply(s, this);
			} catch (final Throwable ex) {
				cleanup(s);
				throw ex;
			}
			
			if (!hasValue) {
				cleanup(s);
				
				if (!terminate) {
					throw new IllegalStateException("The generator didn't call any of the SynchronousSink method");
				}

				Throwable e = generatedError;
				if (e != null) {

					generatedError = null;
					throw Exceptions.propagate(e);
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

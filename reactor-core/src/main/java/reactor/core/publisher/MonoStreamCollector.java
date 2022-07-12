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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Collects the values from the source sequence into a {@link java.util.stream.Collector}
 * instance.
 *
 * @param <T> the source value type
 * @param <A> an intermediate value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoStreamCollector<T, A, R> extends MonoFromFluxOperator<T, R>
		implements Fuseable {

	final Collector<? super T, A, ? extends R> collector;

	MonoStreamCollector(Flux<? extends T> source,
			Collector<? super T, A, ? extends R> collector) {
		super(source);
		this.collector = Objects.requireNonNull(collector, "collector");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		A container = collector.supplier()
		                     .get();

		BiConsumer<? super A, ? super T>  accumulator = collector.accumulator();

		Function<? super A, ? extends R>  finisher = collector.finisher();

		return new StreamCollectorSubscriber<>(actual, container, accumulator, finisher);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class StreamCollectorSubscriber<T, A, R> implements InnerOperator<T, R>,
	                                                                 Fuseable, //for constants only
															         QueueSubscription<R> {

		final BiConsumer<? super A, ? super T> accumulator;

		final Function<? super A, ? extends R> finisher;

		final CoreSubscriber<? super R> actual;

		A container; //not final to be able to null it out on termination

		Subscription s;

		boolean hasRequest;

		boolean done;

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamCollectorSubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(StreamCollectorSubscriber.class, "state");

		StreamCollectorSubscriber(CoreSubscriber<? super R> actual,
				A container,
				BiConsumer<? super A, ? super T> accumulator,
				Function<? super A, ? extends R> finisher) {
			this.actual = actual;
			this.container = container;
			this.accumulator = accumulator;
			this.finisher = finisher;
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		void discardIntermediateContainer(A a) {
			Context ctx = actual.currentContext();
			if (a instanceof Collection) {
				Operators.onDiscardMultiple((Collection<?>) a, ctx);
			}
			else {
				Operators.onDiscard(a, ctx);
			}
		}
		//NB: value and thus discard are not used

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
			try {
				synchronized (this) {
					accumulator.accept(container, t);
				}
			}
			catch (Throwable ex) {
				Context ctx = actual.currentContext();
				Operators.onDiscard(t, ctx);
				onError(Operators.onOperatorError(s, ex, t, ctx)); //discards intermediate container
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			final A c;
			synchronized (this) {
				c = container;
				container = null;
			}

			if (c != null) {
				discardIntermediateContainer(c);
				actual.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			if (hasRequest) {
				final A c;
				synchronized (this) {
					c = container;
					container = null;
				}

				if (c == null) {
					return;
				}

				R r;
				try {
					r = finisher.apply(c);
				}
				catch (Throwable ex) {
					discardIntermediateContainer(c);
					actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
					return;
				}

				if (r == null) {
					actual.onError(Operators.onOperatorError(new NullPointerException(
							"Collector returned null"), actual.currentContext()));
					return;
				}

				actual.onNext(r);
				actual.onComplete();
			}

			final int state = this.state;
			if (state == 0 && STATE.compareAndSet(this, 0, 2)) {
				return;
			}

			final A c;
			synchronized (this) {
				c = container;
				container = null;
			}

			if (c == null) {
				return;
			}

			R r;
			try {
				r = finisher.apply(c);
			}
			catch (Throwable ex) {
				discardIntermediateContainer(c);
				actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
				return;
			}

			if (r == null) {
				actual.onError(Operators.onOperatorError(new NullPointerException(
						"Collector returned null"), actual.currentContext()));
				return;
			}

			actual.onNext(r);
			actual.onComplete();
		}

		@Override
		public void cancel() {
			s.cancel();

			final A c;
			synchronized (this) {
				c = container;
				container = null;
			}
			if (c != null) {
				discardIntermediateContainer(c);
			}
		}

		@Override
		public void request(long n) {
			if (!hasRequest) {
				hasRequest = true;

				final int state = this.state;
				if ((state & 1) == 1) {
					return;
				}

				if (STATE.compareAndSet(this, state, state | 1)) {
					if (state == 0) {
						s.request(Long.MAX_VALUE);
					}
					else {
						// completed before request means source was empty
						final A c;
						synchronized (this) {
							c = container;
							container = null;
						}

						if (c == null) {
							return;
						}

						R r;
						try {
							r = finisher.apply(c);
						}
						catch (Throwable ex) {
							discardIntermediateContainer(c);
							actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
							return;
						}

						if (r == null) {
							actual.onError(Operators.onOperatorError(new NullPointerException(
									"Collector returned null"), actual.currentContext()));
							return;
						}

						actual.onNext(r);
						actual.onComplete();
					}
				}
			}
		}

		@Override
		public int requestFusion(int mode) {
			return Fuseable.NONE;
		}

		@Override
		public R poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}
	}
}

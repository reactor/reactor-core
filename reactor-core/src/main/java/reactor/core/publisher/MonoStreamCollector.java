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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import org.jspecify.annotations.Nullable;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
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

	static final class StreamCollectorSubscriber<T, A, R> extends Operators.BaseFluxToMonoOperator<T, R> {

		final BiConsumer<? super A, ? super T> accumulator;

		final Function<? super A, ? extends R> finisher;

		A container; //not final to be able to null it out on termination

		boolean done;

		StreamCollectorSubscriber(CoreSubscriber<? super R> actual,
				A container,
				BiConsumer<? super A, ? super T> accumulator,
				Function<? super A, ? extends R> finisher) {
			super(actual);
			this.container = container;
			this.accumulator = accumulator;
			this.finisher = finisher;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;

			return super.scanUnsafe(key);
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

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			try {
				synchronized (this) {
					final A container = this.container;
					if (container != null) {
						accumulator.accept(container, t);
						return;
					}
				}
				Operators.onDiscard(t, actual.currentContext());
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

			if (c == null) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			discardIntermediateContainer(c);
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
		public void cancel() {
			super.cancel();

			final A c;
			synchronized (this) {
				c = container;
				container = null;
			}
			if (c != null) {
				discardIntermediateContainer(c);
			}
		}

		R accumulatedValue() {
			final A c;
			synchronized (this) {
				c = container;
				container = null;
			}

			if (c == null) {
				return null;
			}

			R r;
			try {
				r = finisher.apply(c);
			}
			catch (Throwable ex) {
				discardIntermediateContainer(c);
				actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
				return null;
			}

			if (r == null) {
				actual.onError(Operators.onOperatorError(new NullPointerException(
						"Collector returned null"), actual.currentContext()));
			}

			return r;
		}
	}
}

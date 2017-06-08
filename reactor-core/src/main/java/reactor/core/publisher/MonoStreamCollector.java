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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import javax.annotation.Nullable;
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
final class MonoStreamCollector<T, A, R> extends MonoOperator<T, R> implements Fuseable {

	final Collector<? super T, A, ? extends R> collector;

	MonoStreamCollector(Flux<? extends T> source,
			Collector<? super T, A, ? extends R> collector) {
		super(source);
		this.collector = Objects.requireNonNull(collector, "collector");
	}

	@Override
	public void subscribe(Subscriber<? super R> s, Context ctx) {
		A container;
		BiConsumer<? super A, ? super T> accumulator;
		Function<? super A, ? extends R> finisher;

		try {
			container = collector.supplier()
			                     .get();

			accumulator = collector.accumulator();

			finisher = collector.finisher();
		}
		catch (Throwable ex) {
			Operators.error(s, Operators.onOperatorError(ex));
			return;
		}

		source.subscribe(new StreamCollectorSubscriber<>(s,
				container,
				accumulator,
				finisher), ctx);
	}

	static final class StreamCollectorSubscriber<T, A, R>
			extends Operators.MonoSubscriber<T, R> {

		final BiConsumer<? super A, ? super T> accumulator;

		final Function<? super A, ? extends R> finisher;

		A container;

		Subscription s;

		boolean done;

		StreamCollectorSubscriber(Subscriber<? super R> actual,
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
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == ScannableAttr.PARENT) return s;

			return super.scanUnsafe(key);
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
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			try {
				accumulator.accept(container, t);
			}
			catch (Throwable ex) {
				onError(Operators.onOperatorError(s, ex, t));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			container = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			A a = container;
			container = null;

			R r;

			try {
				r = finisher.apply(a);
			}
			catch (Throwable ex) {
				actual.onError(Operators.onOperatorError(ex));
				return;
			}

			complete(r);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

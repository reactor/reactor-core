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
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;
import javax.annotation.Nullable;

/**
 * Repeats a source when a companion sequence signals an item in response to the main's
 * completion signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat attempt
 * is suppressed and any terminal signal will terminate the main source with the same
 * signal immediately.
 *
 * @param <T> the source value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRepeatWhen<T> extends FluxOperator<T, T> {

	final Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory;

	FluxRepeatWhen(ContextualPublisher<? extends T> source,
			Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory =
				Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		RepeatWhenOtherSubscriber other = new RepeatWhenOtherSubscriber();
		Subscriber<Long> signaller = Operators.serialize(other.completionSignal);

		signaller.onSubscribe(Operators.emptySubscription());

		Subscriber<T> serial = Operators.serialize(s);

		RepeatWhenMainSubscriber<T> main =
				new RepeatWhenMainSubscriber<>(serial, signaller, source, ctx);
		other.main = main;

		serial.onSubscribe(main);

		Publisher<?> p;

		try {
			p = Objects.requireNonNull(whenSourceFactory.apply(other),
					"The whenSourceFactory returned a null Publisher");
		}
		catch (Throwable e) {
			s.onError(Operators.onOperatorError(e));
			return;
		}

		p.subscribe(other);

		if (!main.cancelled) {
			source.subscribe(main, ctx);
		}
	}

	static final class RepeatWhenMainSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Operators.DeferredSubscription otherArbiter;

		final Subscriber<Long> signaller;

		final Publisher<? extends T> source;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RepeatWhenMainSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(RepeatWhenMainSubscriber.class,
						"wip");

		long produced;

		RepeatWhenMainSubscriber(Subscriber<? super T> actual,
				Subscriber<Long> signaller,
				Publisher<? extends T> source, Context ctx) {
			super(actual, ctx);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new Operators.DeferredSubscription();
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(signaller), otherArbiter);
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				otherArbiter.cancel();
				super.cancel();
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);

			produced++;
		}

		@Override
		public void onError(Throwable t) {
			otherArbiter.cancel();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			long p = produced;
			if (p != 0L) {
				produced = 0;
				produced(p);
			}

			otherArbiter.request(1);
			signaller.onNext(p);
		}

		void setWhen(Subscription w) {
			otherArbiter.set(w);
		}

		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					source.subscribe(this);

				}
				while (WIP.decrementAndGet(this) != 0);
			}
		}

		void whenError(Throwable e) {
			super.cancel();

			actual.onError(e);
		}

		void whenComplete() {
			super.cancel();

			actual.onComplete();
		}
	}

	static final class RepeatWhenOtherSubscriber extends Flux<Long>
			implements InnerConsumer<Object> {

		RepeatWhenMainSubscriber<?> main;

		final DirectProcessor<Long> completionSignal = new DirectProcessor<>();

		@Override
		public Context currentContext() {
			return main.context;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return main.otherArbiter;
			if (key == ScannableAttr.ACTUAL) return main;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setWhen(s);
		}

		@Override
		public void onNext(Object t) {
			main.resubscribe();
		}

		@Override
		public void onError(Throwable t) {
			main.whenError(t);
		}

		@Override
		public void onComplete() {
			main.whenComplete();
		}

		@Override
		public void subscribe(Subscriber<? super Long> s, Context ctx) {
			ContextRelay.set(s, main.context);
			completionSignal.subscribe(s);
		}

	}
}

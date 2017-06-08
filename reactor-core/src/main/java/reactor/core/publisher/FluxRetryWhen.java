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
 * retries a source when a companion sequence signals
 * an item in response to the main's error signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRetryWhen<T> extends FluxOperator<T, T> {

	final Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory;

	FluxRetryWhen(Flux<? extends T> source,
							  Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	static <T> void subscribe(Subscriber<? super T> s, Function<? super
			Flux<Throwable>, ?
			extends Publisher<?>> whenSourceFactory, Publisher<? extends
			T> source, Context ctx) {
		RetryWhenOtherSubscriber other = new RetryWhenOtherSubscriber();
		Subscriber<Throwable> signaller = Operators.serialize(other.completionSignal);

		signaller.onSubscribe(Operators.emptySubscription());

		Subscriber<T> serial = Operators.serialize(s);

		RetryWhenMainSubscriber<T> main =
				new RetryWhenMainSubscriber<>(serial, signaller, source, ctx);
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
			source.subscribe(main);
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		subscribe(s, whenSourceFactory, source, ctx);
	}

	static final class RetryWhenMainSubscriber<T> extends
	                                              Operators.MultiSubscriptionSubscriber<T, T> {

		final Operators.DeferredSubscription otherArbiter;

		final Subscriber<Throwable> signaller;

		final Publisher<? extends T> source;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RetryWhenMainSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryWhenMainSubscriber.class, "wip");

		long produced;
		
		RetryWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Throwable> signaller,
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

		public void setWhen(Subscription w) {
			otherArbiter.set(w);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);

			produced++;
		}

		@Override
		public void onError(Throwable t) {
			long p = produced;
			if (p != 0L) {
				produced = 0;
				produced(p);
			}

			otherArbiter.request(1);

			signaller.onNext(t);
		}

		@Override
		public void onComplete() {
			otherArbiter.cancel();

			actual.onComplete();
		}

		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
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

	static final class RetryWhenOtherSubscriber extends Flux<Throwable>
	implements InnerConsumer<Object> {
		RetryWhenMainSubscriber<?> main;

		final DirectProcessor<Throwable> completionSignal = new DirectProcessor<>();

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
		public void subscribe(Subscriber<? super Throwable> s, Context ctx) {
			ContextRelay.set(s, main.context);
			completionSignal.subscribe(s);
		}
	}
}

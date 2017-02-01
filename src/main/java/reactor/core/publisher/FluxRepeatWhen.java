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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Loopback;

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
final class FluxRepeatWhen<T> extends FluxSource<T, T> {

	final Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory;

	public FluxRepeatWhen(Publisher<? extends T> source,
			Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory =
				Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		RepeatWhenOtherSubscriber other = new RepeatWhenOtherSubscriber();
		Subscriber<Long> signaller = Operators.serialize(other.completionSignal);

		signaller.onSubscribe(Operators.emptySubscription());

		Subscriber<T> serial = Operators.serialize(s);

		RepeatWhenMainSubscriber<T> main =
				new RepeatWhenMainSubscriber<>(serial, signaller, source);
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

		volatile boolean cancelled;

		long produced;

		public RepeatWhenMainSubscriber(Subscriber<? super T> actual,
				Subscriber<Long> signaller,
				Publisher<? extends T> source) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new Operators.DeferredSubscription();
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			cancelWhen();

			super.cancel();
		}

		@Override
		public void onNext(T t) {
			subscriber.onNext(t);

			produced++;
		}

		@Override
		public void onError(Throwable t) {
			otherArbiter.cancel();

			subscriber.onError(t);
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

		void cancelWhen() {
			otherArbiter.cancel();
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
			cancelled = true;
			super.cancel();

			subscriber.onError(e);
		}

		void whenComplete() {
			cancelled = true;
			super.cancel();

			subscriber.onComplete();
		}
	}

	static final class RepeatWhenOtherSubscriber extends Flux<Long>
			implements Subscriber<Object>, Loopback {

		RepeatWhenMainSubscriber<?> main;

		final DirectProcessor<Long> completionSignal = new DirectProcessor<>();

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
		public void subscribe(Subscriber<? super Long> s) {
			completionSignal.subscribe(s);
		}

		@Override
		public Object connectedInput() {
			return main;
		}

		@Override
		public Object connectedOutput() {
			return completionSignal;
		}
	}
}

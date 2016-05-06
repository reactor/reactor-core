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
import reactor.core.flow.Loopback;
import reactor.core.subscriber.MultiSubscriptionSubscriber;
import reactor.core.subscriber.Subscribers;
import reactor.core.util.DeferredSubscription;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * retries a source when a companion sequence signals
 * an item in response to the main's error signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxRetryWhen<T> extends FluxSource<T, T> {

	final Function<? super Flux<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory;

	public FluxRetryWhen(Publisher<? extends T> source,
							  Function<? super Flux<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	@Override
	public long getCapacity() {
		return -1L;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		RetryWhenOtherSubscriber other = new RetryWhenOtherSubscriber();
		Subscriber<Throwable> signaller = Subscribers.serialize(other.completionSignal);
		
		signaller.onSubscribe(EmptySubscription.INSTANCE);

		Subscriber<T> serial = Subscribers.serialize(s);

		RetryWhenMainSubscriber<T> main = new RetryWhenMainSubscriber<>(serial, signaller, source);
		other.main = main;

		serial.onSubscribe(main);

		Publisher<? extends Object> p;

		try {
			p = whenSourceFactory.apply(other);
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			s.onError(Exceptions.unwrap(e));
			return;
		}

		if (p == null) {
			s.onError(new NullPointerException("The whenSourceFactory returned a null Publisher"));
			return;
		}

		p.subscribe(other);

		if (!main.cancelled) {
			source.subscribe(main);
		}
	}

	static final class RetryWhenMainSubscriber<T> extends MultiSubscriptionSubscriber<T, T> {

		final DeferredSubscription otherArbiter;

		final Subscriber<Throwable> signaller;

		final Publisher<? extends T> source;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RetryWhenMainSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryWhenMainSubscriber.class, "wip");

		volatile boolean cancelled;

		long produced;
		
		public RetryWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Throwable> signaller,
												Publisher<? extends T> source) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new DeferredSubscription();
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

		void cancelWhen() {
			otherArbiter.cancel();
		}

		public void setWhen(Subscription w) {
			otherArbiter.set(w);
		}

		@Override
		public void onNext(T t) {
			subscriber.onNext(t);

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

			subscriber.onComplete();
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

	static final class RetryWhenOtherSubscriber
			extends Flux<Throwable>
	implements Subscriber<Object>, Loopback {
		RetryWhenMainSubscriber<?> main;

		final DirectProcessor<Throwable> completionSignal = new DirectProcessor<>();

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
		public void subscribe(Subscriber<? super Throwable> s) {
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

		@Override
		public int getMode() {
			return INNER | TRACE_ONLY;
		}
	}
}

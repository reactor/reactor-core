/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

/**
 * Retries a source when a companion sequence signals
 * an item in response to the main's error signal.
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRetryWhen<T> extends InternalFluxOperator<T, T> {

	final Retry whenSourceFactory;

	FluxRetryWhen(Flux<? extends T> source, Retry whenSourceFactory) {
		super(source);
		this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	static <T> void subscribe(CoreSubscriber<? super T> s,
			Retry whenSourceFactory,
			CorePublisher<? extends T> source) {
		RetryWhenOtherSubscriber other = new RetryWhenOtherSubscriber();
		Subscriber<Retry.RetrySignal> signaller = Operators.serialize(other.completionSignal);

		signaller.onSubscribe(Operators.emptySubscription());

		CoreSubscriber<T> serial = Operators.serialize(s);

		RetryWhenMainSubscriber<T> main =
				new RetryWhenMainSubscriber<>(serial, signaller, source);

		other.main = main;
		serial.onSubscribe(main);

		Publisher<?> p;
		try {
			p = Objects.requireNonNull(whenSourceFactory.generateCompanion(other), "The whenSourceFactory returned a null Publisher");
		}
		catch (Throwable e) {
			s.onError(Operators.onOperatorError(e, s.currentContext()));
			return;
		}
		p.subscribe(other);

		if (!main.cancelled) {
			source.subscribe(main);
		}
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		subscribe(actual, whenSourceFactory, source);
		return null;
	}

	static final class RetryWhenMainSubscriber<T> extends Operators.MultiSubscriptionSubscriber<T, T>
			implements Retry.RetrySignal {

		final Operators.DeferredSubscription otherArbiter;

		final Subscriber<Retry.RetrySignal> signaller;

		final CorePublisher<? extends T> source;

		long totalFailureIndex = 0L;
		long subsequentFailureIndex = 0L;
		@Nullable
		Throwable lastFailure = null;

		Context context;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<RetryWhenMainSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryWhenMainSubscriber.class, "wip");

		long produced;
		
		RetryWhenMainSubscriber(CoreSubscriber<? super T> actual,
				Subscriber<Retry.RetrySignal> signaller,
				CorePublisher<? extends T> source) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new Operators.DeferredSubscription();
			this.context = actual.currentContext();
		}

		@Override
		public long totalRetries() {
			return this.totalFailureIndex - 1;
		}

		@Override
		public long totalRetriesInARow() {
			return this.subsequentFailureIndex - 1;
		}

		@Override
		public Throwable failure() {
			assert this.lastFailure != null;
			return this.lastFailure;
		}

		@Override
		public Context currentContext() {
			return this.context;
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

		void swap(Subscription w) {
			otherArbiter.set(w);
		}

		@Override
		public void onNext(T t) {
			subsequentFailureIndex = 0;
			actual.onNext(t);

			produced++;
		}

		@Override
		public void onError(Throwable t) {
			totalFailureIndex++;
			subsequentFailureIndex++;
			lastFailure = t;
			long p = produced;
			if (p != 0L) {
				produced = 0;
				produced(p);
			}

			otherArbiter.request(1);

			signaller.onNext(this);
		}

		@Override
		public void onComplete() {
			lastFailure = null;
			otherArbiter.cancel();

			actual.onComplete();
		}

		void resubscribe(Object trigger) {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					//flow that emit a Context as a trigger for the re-subscription are
					//used to REPLACE the currentContext()
					if (trigger instanceof Context) {
						this.context = this.context.putAll((Context) trigger);
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

	static final class RetryWhenOtherSubscriber extends Flux<Retry.RetrySignal>
	implements InnerConsumer<Object>, OptimizableOperator<Retry.RetrySignal, Retry.RetrySignal> {
		RetryWhenMainSubscriber<?> main;

		final FluxProcessor<Retry.RetrySignal, Retry.RetrySignal> completionSignal = Processors.more().multicastNoBackpressure();

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main.otherArbiter;
			if (key == Attr.ACTUAL) return main;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.swap(s);
		}

		@Override
		public void onNext(Object t) {
			main.resubscribe(t);
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
		public void subscribe(CoreSubscriber<? super Retry.RetrySignal> actual) {
			completionSignal.subscribe(actual);
		}

		@Override
		public CoreSubscriber<? super Retry.RetrySignal> subscribeOrReturn(CoreSubscriber<? super Retry.RetrySignal> actual) {
			return actual;
		}

		@Override
		public CorePublisher<Retry.RetrySignal> source() {
			return completionSignal;
		}

		@Override
		public OptimizableOperator<?, ? extends Retry.RetrySignal> nextOptimizableSource() {
			return null;
		}
	}

}

/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.repeat.Repeat;

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
final class FluxRepeatWhen<T> extends InternalFluxOperator<T, T> {

	final Repeat whenSourceFactory;

	FluxRepeatWhen(Flux<? extends T> source,
			Repeat whenSourceFactory) {
		super(source);
		this.whenSourceFactory =
				Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	static <T> void subscribe(CoreSubscriber<? super T> actual,
							  Repeat whenSourceFactory,
							  CorePublisher<? extends T> source) {
		RepeatWhenOtherSubscriber other = new RepeatWhenOtherSubscriber();
		CoreSubscriber<T> serial = Operators.serialize(actual);

		RepeatWhenMainSubscriber<T> main = new RepeatWhenMainSubscriber<>(
				serial, other.completionSignal, source, whenSourceFactory.getRepeatContext());
		other.main = main;

		serial.onSubscribe(main);

		Publisher<?> p;

		try {
			p = Objects.requireNonNull(whenSourceFactory.generateCompanion(other),
					"The whenSourceFactory returned a null Publisher");
		}
		catch (Throwable e) {
			actual.onError(Operators.onOperatorError(e, actual.currentContext()));
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

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class RepeatWhenMainSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T>
			implements Repeat.RepeatSignal {

		final Operators.DeferredSubscription otherArbiter;

		final Sinks.Many<Repeat.RepeatSignal> signaller;

		final CorePublisher<? extends T> source;

		long totalRetriesSoFar = 0L;
		final ContextView repeatContext;
		volatile int wip;
		static final AtomicIntegerFieldUpdater<RepeatWhenMainSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(RepeatWhenMainSubscriber.class,
						"wip");

		Context context;
		long    produced;

		RepeatWhenMainSubscriber(CoreSubscriber<? super T> actual,
								 Sinks.Many<Repeat.RepeatSignal> signaller,
								 CorePublisher<? extends T> source, ContextView repeatContext) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.repeatContext = repeatContext;
			this.otherArbiter = new Operators.DeferredSubscription();
			this.context = actual.currentContext();
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

		@Override
		public void onNext(T t) {
			totalRetriesSoFar++;
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

			signaller.emitNext(this, Sinks.EmitFailureHandler.FAIL_FAST);
			// request after signalling, otherwise it may race
			otherArbiter.request(1);
		}

		void setWhen(Subscription w) {
			otherArbiter.set(w);
		}

		void resubscribe(Object trigger) {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					//flow that emit a Context as a trigger for the re-subscription are
					//used to REPLACE the currentContext()
					if (trigger instanceof ContextView) {
						this.context = this.context.putAll((ContextView) trigger);
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

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}

		@Override
		public Throwable failure() {
			return null;
		}

		@Override
		public ContextView retryContextView() {
			return repeatContext;
		}

		@Override
		public long getRepeatsSoFar() {
			return totalRetriesSoFar;
		}
	}

	static final class RepeatWhenOtherSubscriber extends Flux<Repeat.RepeatSignal>
			implements InnerConsumer<Object>, OptimizableOperator<Repeat.RepeatSignal, Repeat.RepeatSignal>
	{

		RepeatWhenMainSubscriber<?> main;

		final Sinks.Many<Repeat.RepeatSignal> completionSignal = Sinks.many().multicast().onBackpressureBuffer();

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main.otherArbiter;
			if (key == Attr.ACTUAL) return main;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setWhen(s);
		}

		@Override
		public void onNext(Object t) {
			if (t.equals(-1L)) {
				onComplete();
			} else {
				main.resubscribe(t);
			}
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
		public void subscribe(CoreSubscriber<? super Repeat.RepeatSignal> actual) {
			completionSignal.asFlux().subscribe(actual);
		}

		@Override
		public CoreSubscriber<? super Repeat.RepeatSignal> subscribeOrReturn(CoreSubscriber<? super Repeat.RepeatSignal> actual) {
			return actual;
		}

		@Override
		public Flux<Repeat.RepeatSignal> source() {
			return completionSignal.asFlux();
		}

		@Override
		public OptimizableOperator<?, ? extends Repeat.RepeatSignal> nextOptimizableSource() {
			return null;
		}
	}
}

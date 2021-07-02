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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Takes a value from upstream then uses the duration provided by a
 * generated Publisher to skip other values until that other Publisher signals.
 *
 * @param <T> the source and output value type
 * @param <U> the value type of the publisher signalling the end of the throttling
 * duration
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSampleFirst<T, U> extends InternalFluxOperator<T, T> {

	final Function<? super T, ? extends Publisher<U>> throttler;

	FluxSampleFirst(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<U>> throttler) {
		super(source);
		this.throttler = Objects.requireNonNull(throttler, "throttler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		SampleFirstMain<T, U> main = new SampleFirstMain<>(actual, throttler);

		actual.onSubscribe(main);

		return main;
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SampleFirstMain<T, U> implements InnerOperator<T, T> {

		final Function<? super T, ? extends Publisher<U>> throttler;
		final CoreSubscriber<? super T>                   actual;
		final Context                                     ctx;

		volatile boolean gate;

		volatile Subscription s;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleFirstMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SampleFirstMain.class,
						Subscription.class,
						"s");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleFirstMain, Subscription> OTHER =
				AtomicReferenceFieldUpdater.newUpdater(SampleFirstMain.class,
						Subscription.class,
						"other");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SampleFirstMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SampleFirstMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SampleFirstMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SampleFirstMain.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleFirstMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SampleFirstMain.class,
						Throwable.class,
						"error");

		SampleFirstMain(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<U>> throttler) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.throttler = throttler;
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(other));
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
			Operators.terminate(OTHER, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (!gate) {
				gate = true;

				if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
					actual.onNext(t);
					if (WIP.decrementAndGet(this) != 0) {
						handleTermination();
						return;
					}
				}
				else {
					return;
				}

				Publisher<U> p;

				try {
					p = Objects.requireNonNull(throttler.apply(t),
							"The throttler returned a null publisher");
				}
				catch (Throwable e) {
					Operators.terminate(S, this);
					error(Operators.onOperatorError(null, e, t, ctx));
					return;
				}

				SampleFirstOther<U> other = new SampleFirstOther<>(this);

				if (Operators.replace(OTHER, this, other)) {
					p.subscribe(other);
				}
			}
			else {
				Operators.onDiscard(t, ctx);
			}
		}

		void handleTermination() {
			Throwable e = Exceptions.terminate(ERROR, this);
			if (e != null && e != Exceptions.TERMINATED) {
				actual.onError(e);
			}
			else {
				actual.onComplete();
			}
		}

		void error(Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				if (WIP.getAndIncrement(this) == 0) {
					handleTermination();
				}
			}
			else {
				Operators.onErrorDropped(e, ctx);
			}
		}

		@Override
		public void onError(Throwable t) {
			Operators.terminate(OTHER, this);

			error(t);
		}

		@Override
		public void onComplete() {
			Operators.terminate(OTHER, this);

			if (WIP.getAndIncrement(this) == 0) {
				handleTermination();
			}
		}

		void otherNext() {
			gate = false;
		}

		void otherError(Throwable e) {
			Operators.terminate(S, this);

			error(e);
		}
	}

	static final class SampleFirstOther<U> extends Operators.DeferredSubscription
			implements InnerConsumer<U> {

		final SampleFirstMain<?, U> main;

		SampleFirstOther(SampleFirstMain<?, U> main) {
			this.main = main;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return main;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			cancel();

			main.otherNext();
		}

		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}

		@Override
		public void onComplete() {
			main.otherNext();
		}

	}
}

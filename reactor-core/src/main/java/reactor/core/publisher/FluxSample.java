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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Samples the main source and emits its latest value whenever the other Publisher
 * signals a value.
 * <p>
 * <p>
 * Termination of either Publishers will result in termination for the Subscriber
 * as well.
 * <p>
 * <p>
 * Both Publishers will run in unbounded mode because the backpressure
 * would interfere with the sampling precision.
 * 
 * @param <T> the input and output value type
 * @param <U> the value type of the sampler (irrelevant)
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSample<T, U> extends InternalFluxOperator<T, T> {

	final Publisher<U> other;

	FluxSample(Flux<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		CoreSubscriber<T> serial = Operators.serialize(actual);

		SampleMainSubscriber<T> main = new SampleMainSubscriber<>(serial);

		actual.onSubscribe(main);

		other.subscribe(new SampleOther<>(main));
		return main;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SampleMainSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Context ctx;

		volatile T                  value;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleMainSubscriber, Object> VALUE =
		  AtomicReferenceFieldUpdater.newUpdater(SampleMainSubscriber.class, Object.class, "value");

		volatile Subscription main;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleMainSubscriber, Subscription> MAIN =
		  AtomicReferenceFieldUpdater.newUpdater(SampleMainSubscriber.class, Subscription.class, "main");
		volatile Subscription other;


		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleMainSubscriber, Subscription> OTHER =
		  AtomicReferenceFieldUpdater.newUpdater(SampleMainSubscriber.class, Subscription.class, "other");
		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SampleMainSubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(SampleMainSubscriber.class, "requested");

		SampleMainSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
			this.ctx = actual.currentContext();
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
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.PARENT) return main;
			if (key == Attr.CANCELLED) return main == Operators.cancelledSubscription();
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
				return;
			}
			s.request(Long.MAX_VALUE);
		}

		void cancelMain() {
			Subscription s = main;
			if (s != Operators.cancelledSubscription()) {
				s = MAIN.getAndSet(this, Operators.cancelledSubscription());
				if (s != null && s != Operators.cancelledSubscription()) {
					s.cancel();
				}
			}
		}

		void cancelOther() {
			Subscription s = other;
			if (s != Operators.cancelledSubscription()) {
				s = OTHER.getAndSet(this, Operators.cancelledSubscription());
				if (s != null && s != Operators.cancelledSubscription()) {
					s.cancel();
				}
			}
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
				return;
			}
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onNext(T t) {
			Object old = VALUE.getAndSet(this, t);
			if (old != null) {
				Operators.onDiscard(old, ctx);
			}
		}

		@Override
		public void onError(Throwable t) {
			cancelOther();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			cancelOther();
			//implementation note: if the sampler triggers at the exact same time as the
			// last source value being emitted, it could be that the sampler "wins" and
			// suppresses the value by cancelling the main. In any other case, the block
			// below ensures last source value is always part of the sample.
			T v = value;
			if (v != null) {
				actual.onNext(value);
			}

			actual.onComplete();
		}

		@SuppressWarnings("unchecked")
		@Nullable
		T getAndNullValue() {
			return (T) VALUE.getAndSet(this, null);
		}

		void decrement() {
			REQUESTED.decrementAndGet(this);
		}
	}

	static final class SampleOther<T, U> implements InnerConsumer<U> {
		final SampleMainSubscriber<T> main;

		SampleOther(SampleMainSubscriber<T> main) {
			this.main = main;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main.other;
			if (key == Attr.ACTUAL) return main;
			if (key == Attr.CANCELLED) return main.other == Operators.cancelledSubscription();
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);
		}

		@Override
		public void onNext(U t) {
			SampleMainSubscriber<T> m = main;

			T v = m.getAndNullValue();

			if (v != null) {
				if (m.requested != 0L) {
					m.actual.onNext(v);

					if (m.requested != Long.MAX_VALUE) {
						m.decrement();
					}
					return;
				}

				m.cancel();
				m.actual.onError(Exceptions.failWithOverflow("Can't signal value due to lack of requests"));
				Operators.onDiscard(v, m.ctx);
			}
		}

		@Override
		public void onError(Throwable t) {
			SampleMainSubscriber<T> m = main;

			m.cancelMain();

			m.actual.onError(t);
		}

		@Override
		public void onComplete() {
			SampleMainSubscriber<T> m = main;

			m.cancelMain();

			m.actual.onComplete();
		}


	}
}

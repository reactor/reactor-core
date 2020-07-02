/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

/**
 * Combines values from a main Publisher with values from another
 * Publisher through a bi-function and emits the result.
 * <p>
 * <p>
 * The operator will drop values from the main source until the other
 * Publisher produces any value.
 * <p>
 * If the other Publisher completes without any value, the sequence is completed.
 *
 * @param <T> the main source type
 * @param <U> the alternate source type
 * @param <R> the output type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWithLatestFrom<T, U, R> extends InternalFluxOperator<T, R> {

	final Publisher<? extends U> other;

	final BiFunction<? super T, ? super U, ? extends R> combiner;

	FluxWithLatestFrom(Flux<? extends T> source,
			Publisher<? extends U> other,
			BiFunction<? super T, ? super U, ? extends R> combiner) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		CoreSubscriber<R> serial = Operators.serialize(actual);

		WithLatestFromSubscriber<T, U, R> main =
				new WithLatestFromSubscriber<>(serial, combiner);

		WithLatestFromOtherSubscriber<U> secondary =
				new WithLatestFromOtherSubscriber<>(main);

		other.subscribe(secondary);

		return main;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == RUN_STYLE) return SYNC;
		return super.scanUnsafe(key);
	}

	static final class WithLatestFromSubscriber<T, U, R> implements InnerOperator<T, R> {

		final CoreSubscriber<? super R> actual;
		final BiFunction<? super T, ? super U, ? extends R> combiner;

		volatile Subscription main;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WithLatestFromSubscriber, Subscription>
				MAIN =
				AtomicReferenceFieldUpdater.newUpdater(WithLatestFromSubscriber.class,
						Subscription.class,
						"main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WithLatestFromSubscriber, Subscription>
				OTHER =
				AtomicReferenceFieldUpdater.newUpdater(WithLatestFromSubscriber.class,
						Subscription.class,
						"other");

		volatile U otherValue;

		WithLatestFromSubscriber(CoreSubscriber<? super R> actual,
				BiFunction<? super T, ? super U, ? extends R> combiner) {
			this.actual = actual;
			this.combiner = combiner;
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return main == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return main;
			if (key == RUN_STYLE) return SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(other));
		}

		@Override
		public void request(long n) {
			main.request(n);
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

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
			else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			U u = otherValue;

			if (u != null) {
				R r;

				try {
					r = Objects.requireNonNull(combiner.apply(t, u),
					"The combiner returned a null value");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(this, e, t, actual.currentContext()));
					return;
				}

				actual.onNext(r);
			}
			else {
				main.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, Operators.cancelledSubscription())) {
					cancelOther();

					Operators.error(actual, t);
					return;
				}
			}
			cancelOther();

			otherValue = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			cancelOther();

			otherValue = null;
			actual.onComplete();
		}

		void otherError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, Operators.cancelledSubscription())) {
					cancelMain();

					Operators.error(actual, t);
					return;
				}
			}
			cancelMain();

			otherValue = null;
			actual.onError(t);
		}

		void otherComplete() {
			if (otherValue == null) {
				if (main == null) {
					if (MAIN.compareAndSet(this,
							null,
							Operators.cancelledSubscription())) {
						cancelMain();

						Operators.complete(actual);
						return;
					}
				}
				cancelMain();

				actual.onComplete();
			}
		}
	}

	static final class WithLatestFromOtherSubscriber<U> implements InnerConsumer<U> {

		final WithLatestFromSubscriber<?, U, ?> main;

		 WithLatestFromOtherSubscriber(WithLatestFromSubscriber<?, U, ?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return main;
			}
			if (key == RUN_STYLE) {
			    return SYNC;
			}
			return null;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		public void onNext(U t) {
			main.otherValue = t;
		}

		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}

		@Override
		public void onComplete() {
			main.otherComplete();
		}
	}
}

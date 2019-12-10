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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item
 * generated Publisher source fires an item or completes before the next item
 * arrives from the main source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTimeout<T, U, V> extends InternalFluxOperator<T, T> {

	final Publisher<U> firstTimeout;

	final Function<? super T, ? extends Publisher<V>> itemTimeout;

	final Publisher<? extends T> other;

	final String timeoutDescription; //only useful when no `other`

	FluxTimeout(Flux<? extends T> source,
			Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> itemTimeout,
			String timeoutDescription) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
		this.other = null;

		this.timeoutDescription = addNameToTimeoutDescription(source,
				Objects.requireNonNull(timeoutDescription, "timeoutDescription is needed when no fallback"));
	}

	FluxTimeout(Flux<? extends T> source,
			Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> itemTimeout,
			Publisher<? extends T> other) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
		this.other = Objects.requireNonNull(other, "other");
		this.timeoutDescription = null;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		CoreSubscriber<T> serial = Operators.serialize(actual);

		TimeoutMainSubscriber<T, V> main =
				new TimeoutMainSubscriber<>(serial, itemTimeout, other, timeoutDescription);

		serial.onSubscribe(main);

		TimeoutTimeoutSubscriber ts = new TimeoutTimeoutSubscriber(main, 0L);

		main.setTimeout(ts);

		firstTimeout.subscribe(ts);

		return main;
	}

	@Nullable
	static String addNameToTimeoutDescription(Publisher<?> source,
			@Nullable  String timeoutDescription) {
		if (timeoutDescription == null) {
			return null;
		}

		Scannable s = Scannable.from(source);
		if (s.isScanAvailable()) {
			return timeoutDescription + " in '" + s.name() + "'";
		}
		else {
			return timeoutDescription;
		}
	}

	static final class TimeoutMainSubscriber<T, V>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Function<? super T, ? extends Publisher<V>> itemTimeout;

		final Publisher<? extends T> other;
		final String timeoutDescription; //only useful/non-null when no `other`

		Subscription s;

		volatile IndexedCancellable timeout;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<TimeoutMainSubscriber, IndexedCancellable>
				TIMEOUT =
				AtomicReferenceFieldUpdater.newUpdater(TimeoutMainSubscriber.class,
						IndexedCancellable.class,
						"timeout");

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TimeoutMainSubscriber> INDEX =
				AtomicLongFieldUpdater.newUpdater(TimeoutMainSubscriber.class, "index");

		TimeoutMainSubscriber(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<V>> itemTimeout,
				@Nullable Publisher<? extends T> other,
				@Nullable String timeoutDescription) {
			super(actual);
			this.itemTimeout = itemTimeout;
			this.other = other;
			this.timeoutDescription = timeoutDescription;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				set(s);
			}
		}

		@Override
		protected boolean shouldCancelCurrent() {
			return true;
		}

		@Override
		public void onNext(T t) {
			timeout.cancel();

			long idx = index;
			if (idx == Long.MIN_VALUE) {
				s.cancel();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (!INDEX.compareAndSet(this, idx, idx + 1)) {
				s.cancel();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			actual.onNext(t);

			producedOne();

			Publisher<? extends V> p;

			try {
				p = Objects.requireNonNull(itemTimeout.apply(t),
						"The itemTimeout returned a null Publisher");
			}
			catch (Throwable e) {
				actual.onError(Operators.onOperatorError(this, e, t,
						actual.currentContext()));
				return;
			}

			TimeoutTimeoutSubscriber ts = new TimeoutTimeoutSubscriber(this, idx + 1);

			if (!setTimeout(ts)) {
				return;
			}

			p.subscribe(ts);
		}

		@Override
		public void onError(Throwable t) {
			long idx = index;
			if (idx == Long.MIN_VALUE) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			cancelTimeout();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			long idx = index;
			if (idx == Long.MIN_VALUE) {
				return;
			}
			if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
				return;
			}

			cancelTimeout();

			actual.onComplete();
		}

		void cancelTimeout() {
			IndexedCancellable s = timeout;
			if (s != CancelledIndexedCancellable.INSTANCE) {
				s = TIMEOUT.getAndSet(this, CancelledIndexedCancellable.INSTANCE);
				if (s != null && s != CancelledIndexedCancellable.INSTANCE) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			index = Long.MIN_VALUE;
			cancelTimeout();
			super.cancel();
		}

		boolean setTimeout(IndexedCancellable newTimeout) {

			for (; ; ) {
				IndexedCancellable currentTimeout = timeout;

				if (currentTimeout == CancelledIndexedCancellable.INSTANCE) {
					newTimeout.cancel();
					return false;
				}

				if (currentTimeout != null && currentTimeout.index() >= newTimeout.index()) {
					newTimeout.cancel();
					return false;
				}

				if (TIMEOUT.compareAndSet(this, currentTimeout, newTimeout)) {
					if (currentTimeout != null) {
						currentTimeout.cancel();
					}
					return true;
				}
			}
		}

		void doTimeout(long i) {
			if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
				handleTimeout();
			}
		}

		void doError(long i, Throwable e) {
			if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
				super.cancel();

				actual.onError(e);
			}
		}

		void handleTimeout() {
			if (other == null) {
				super.cancel();
				actual.onError(new TimeoutException("Did not observe any item or terminal signal within "
						+ timeoutDescription + " (and no fallback has been configured)"));
			}
			else {
				set(Operators.emptySubscription());

				other.subscribe(new TimeoutOtherSubscriber<>(actual, this));
			}
		}
	}

	static final class TimeoutOtherSubscriber<T> implements CoreSubscriber<T> {

		final CoreSubscriber<? super T> actual;

		final Operators.MultiSubscriptionSubscriber<T, T> arbiter;

		TimeoutOtherSubscriber(CoreSubscriber<? super T> actual,
				Operators.MultiSubscriptionSubscriber<T, T> arbiter) {
			this.actual = actual;
			this.arbiter = arbiter;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			arbiter.set(s);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}
	}

	interface IndexedCancellable {

		long index();

		void cancel();
	}

	enum CancelledIndexedCancellable implements IndexedCancellable {
		INSTANCE;

		@Override
		public long index() {
			return Long.MAX_VALUE;
		}

		@Override
		public void cancel() {

		}

	}

	static final class TimeoutTimeoutSubscriber
			implements Subscriber<Object>, IndexedCancellable {

		final TimeoutMainSubscriber<?, ?> main;

		final long index;

		volatile Subscription s;

		static final AtomicReferenceFieldUpdater<TimeoutTimeoutSubscriber, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(TimeoutTimeoutSubscriber.class,
				Subscription.class,
				"s");

		TimeoutTimeoutSubscriber(TimeoutMainSubscriber<?, ?> main, long index) {
			this.main = main;
			this.index = index;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!S.compareAndSet(this, null, s)) {
				s.cancel();
				if (this.s != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
			else {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Object t) {
			s.cancel();

			main.doTimeout(index);
		}

		@Override
		public void onError(Throwable t) {
			main.doError(index, t);
		}

		@Override
		public void onComplete() {
			main.doTimeout(index);
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
		}

		@Override
		public long index() {
			return index;
		}
	}
}

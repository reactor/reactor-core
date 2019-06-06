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
package reactor.test.publisher;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

/**
 * A default implementation of a {@link TestPublisher}.
 *
 * @author Simon Basle
 * @author Stephane Maldini
 */
class DefaultTestPublisher<T> extends TestPublisher<T> {

	@SuppressWarnings("rawtypes")
	private static final TestPublisherSubscription[] EMPTY = new TestPublisherSubscription[0];

	@SuppressWarnings("rawtypes")
	private static final TestPublisherSubscription[] TERMINATED = new TestPublisherSubscription[0];

	volatile int cancelCount;

	static final AtomicIntegerFieldUpdater<DefaultTestPublisher> CANCEL_COUNT =
		AtomicIntegerFieldUpdater.newUpdater(DefaultTestPublisher.class, "cancelCount");

	Throwable error;

	volatile boolean hasOverflown;
	volatile boolean wasRequested;

	volatile long subscribeCount;
	static final AtomicLongFieldUpdater<DefaultTestPublisher> SUBSCRIBED_COUNT =
			AtomicLongFieldUpdater.newUpdater(DefaultTestPublisher.class, "subscribeCount");

	final EnumSet<Violation> violations;

	@SuppressWarnings("unchecked")
	volatile TestPublisherSubscription<T>[] subscribers = EMPTY;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<DefaultTestPublisher, TestPublisherSubscription[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(DefaultTestPublisher.class, TestPublisherSubscription[].class, "subscribers");

	DefaultTestPublisher(Violation first, Violation... rest) {
		this.violations = EnumSet.of(first, rest);
	}

	DefaultTestPublisher() {
		this.violations = EnumSet.noneOf(Violation.class);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		TestPublisherSubscription<T>
				p = new TestPublisherSubscription<>(s, this);
		s.onSubscribe(p);
		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
			DefaultTestPublisher.SUBSCRIBED_COUNT.incrementAndGet(this);

			if (replayOnSubscribe != null) {
				replayOnSubscribe.accept(this);
			}

		} else {
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			} else {
				s.onComplete();
			}
		}
	}

	boolean add(TestPublisherSubscription<T> s) {
		TestPublisherSubscription<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") TestPublisherSubscription<T>[] b = new TestPublisherSubscription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(TestPublisherSubscription<T> s) {
		TestPublisherSubscription<T>[] a = subscribers;

		if (violations.contains(Violation.CLEANUP_ON_TERMINATE)) {
			return;
		}

		if (a == TERMINATED || a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int len = a.length;

			int j = -1;

			for (int i = 0; i < len; i++) {
				if (a[i] == s) {
					j = i;
					break;
				}
			}
			if (j < 0) {
				return;
			}
			if (len == 1) {
				subscribers = EMPTY;
				return;
			}

			TestPublisherSubscription<T>[] b = new TestPublisherSubscription[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	static final class TestPublisherSubscription<T> implements Subscription {

		final Subscriber<? super T>                     actual;
		final Fuseable.ConditionalSubscriber<? super T> actualConditional;

		final DefaultTestPublisher<T> parent;

		volatile boolean cancelled;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TestPublisherSubscription>
				REQUESTED =
				AtomicLongFieldUpdater.newUpdater(TestPublisherSubscription.class, "requested");

		@SuppressWarnings("unchecked")
		TestPublisherSubscription(Subscriber<? super T> actual, DefaultTestPublisher<T> parent) {
			this.actual = actual;
			if(actual instanceof Fuseable.ConditionalSubscriber){
				this.actualConditional = (Fuseable.ConditionalSubscriber<? super T>) actual;
			}
			else {
				this.actualConditional = null;
			}
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				parent.wasRequested = true;
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				DefaultTestPublisher.CANCEL_COUNT.incrementAndGet(parent);
				if (parent.violations.contains(Violation.CLEANUP_ON_TERMINATE)||parent.violations.contains(Violation.DEFER_CANCELLATION)) {
					return;
				}
				cancelled = true;
				parent.remove(this);
			}
		}

		void onNext(T value) {
			long r = requested;
			if (r != 0L || parent.violations.contains(Violation.REQUEST_OVERFLOW)) {
				if (r == 0) {
					parent.hasOverflown = true;
				}
				boolean sent;
				if(actualConditional != null){
					sent = actualConditional.tryOnNext(value);
				}
				else {
					sent = true;
					actual.onNext(value);
				}
				if (sent && r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return;
			}
			parent.remove(this);
			actual.onError(new IllegalStateException("Can't deliver value due to lack of requests"));
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}
	}

	private Consumer<TestPublisher<T>> replayOnSubscribe = null;

	public TestPublisher<T> replayOnSubscribe(Consumer<TestPublisher<T>> replay) {
		if (replayOnSubscribe == null) {
			replayOnSubscribe = replay;
		}
		else {
			replayOnSubscribe = replayOnSubscribe.andThen(replay);
		}
		return this;
	}

	@Override
	public Flux<T> flux() {
		return Flux.from(this);
	}

	@Override
	public boolean wasSubscribed() {
		return subscribeCount > 0;
	}

    @Override
    public long subscribeCount() {
        return subscribeCount;
    }

    @Override
	public boolean wasCancelled() {
		return cancelCount > 0;
	}

	@Override
	public boolean wasRequested() {
		return wasRequested;
	}

	@Override
	public Mono<T> mono() {
		if (violations.isEmpty()) {
			return Mono.from(this);
		}
		else {
			return Mono.fromDirect(this);
		}
	}

	@Override
	public DefaultTestPublisher<T> assertMinRequested(long n) {
		TestPublisherSubscription<T>[] subs = subscribers;
		long minRequest = Stream.of(subs)
		                        .mapToLong(s -> s.requested)
		                        .min()
		                        .orElse(0);
		if (minRequest < n) {
			throw new AssertionError("Expected smallest requested amount to be >= " + n + "; got " + minRequest);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertMaxRequested(long n) {
		TestPublisherSubscription<T>[] subs = subscribers;
		long maxRequest = Stream.of(subs)
		                        .mapToLong(s -> s.requested)
		                        .max()
		                        .orElse(0);
		if (maxRequest > n) {
			throw new AssertionError("Expected largest requested amount to be <= " + n + "; got " + maxRequest);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertSubscribers() {
		TestPublisherSubscription<T>[] s = subscribers;
		if (s == EMPTY || s == TERMINATED) {
			throw new AssertionError("Expected subscribers");
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertSubscribers(int n) {
		int sl = subscribers.length;
		if (sl != n) {
			throw new AssertionError("Expected " + n + " subscribers, got " + sl);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertNoSubscribers() {
		int sl = subscribers.length;
		if (sl != 0) {
			throw new AssertionError("Expected no subscribers, got " + sl);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertCancelled() {
		if (cancelCount == 0) {
			throw new AssertionError("Expected at least 1 cancellation");
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertCancelled(int n) {
		int cc = cancelCount;
		if (cc != n) {
			throw new AssertionError("Expected " + n + " cancellations, got " + cc);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertNotCancelled() {
		if (cancelCount != 0) {
			throw new AssertionError("Expected no cancellation");
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertRequestOverflow() {
		if (!hasOverflown) {
			throw new AssertionError("Expected some request overflow");
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> assertNoRequestOverflow() {
		if (hasOverflown) {
			throw new AssertionError("Unexpected request overflow");
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> next(@Nullable T t) {
		if (!violations.contains(Violation.ALLOW_NULL)) {
			Objects.requireNonNull(t, "emitted values must be non-null");
		}

		for (TestPublisherSubscription<T> s : subscribers) {
			s.onNext(t);
		}

		return this;
	}

	@Override
	public DefaultTestPublisher<T> error(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		TestPublisherSubscription<?>[] subs;
		if (violations.contains(Violation.CLEANUP_ON_TERMINATE)) {
			subs = subscribers;
		} else {
			subs = SUBSCRIBERS.getAndSet(this, TERMINATED);
		}
		for (TestPublisherSubscription<?> s : subs) {
			s.onError(t);
		}
		return this;
	}

	@Override
	public DefaultTestPublisher<T> complete() {
		TestPublisherSubscription<?>[] subs;
		if (violations.contains(Violation.CLEANUP_ON_TERMINATE)) {
			subs = subscribers;
		} else {
			subs = SUBSCRIBERS.getAndSet(this, TERMINATED);
		}
		for (TestPublisherSubscription<?> s : subs) {
			s.onComplete();
		}
		return this;
	}

}

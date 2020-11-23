/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test.publisher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

import static reactor.test.publisher.TestPublisher.Violation.ALLOW_NULL;
import static reactor.test.publisher.TestPublisher.Violation.DEFER_CANCELLATION;
import static reactor.test.publisher.TestPublisher.Violation.REQUEST_OVERFLOW;

/**
 * A cold implementation of a {@link TestPublisher}.
 *
 * @author Simon Basle
 * @author Stephane Maldini
 */
final class ColdTestPublisher<T> extends TestPublisher<T> {

	@SuppressWarnings("rawtypes")
	private static final ColdTestPublisherSubscription[] EMPTY = new ColdTestPublisherSubscription[0];

	final List<T> values;

	/** Non-null if either {@link #error(Throwable) or {@link #complete()}} have been called. */
	Throwable error;

	/** If true, emit an overflow error when there is more values than request. If false, buffer until data is requested. */
	final boolean errorOnOverflow;

	/** If misbehaving, the set of violations this publisher will exhibit. */
	final EnumSet<Violation> violations;

	volatile boolean hasOverflown;
	volatile boolean wasRequested;

	@SuppressWarnings("unchecked")
	volatile ColdTestPublisherSubscription<T>[] subscribers = EMPTY;

	volatile int cancelCount;
	static final AtomicIntegerFieldUpdater<ColdTestPublisher> CANCEL_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(ColdTestPublisher.class, "cancelCount");

	volatile long subscribeCount;
	static final AtomicLongFieldUpdater<ColdTestPublisher> SUBSCRIBED_COUNT =
			AtomicLongFieldUpdater.newUpdater(ColdTestPublisher.class, "subscribeCount");

	ColdTestPublisher(boolean errorOnOverflow, EnumSet<Violation> violations) {
		this.errorOnOverflow = errorOnOverflow;
		this.values = Collections.synchronizedList(new ArrayList<>());
		this.violations = violations;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		ColdTestPublisherSubscription<T> p = new ColdTestPublisherSubscription<>(s, this);
		add(p);
		if (p.cancelled) {
			remove(p);
		}
		ColdTestPublisher.SUBSCRIBED_COUNT.incrementAndGet(this);

		s.onSubscribe(p); // will trigger drain() via request()

	}

	void add(ColdTestPublisherSubscription<T> s) {
		synchronized (this) {
			ColdTestPublisherSubscription<T>[] a = subscribers;
			int len = a.length;

			@SuppressWarnings("unchecked")
			ColdTestPublisherSubscription<T>[] b = new ColdTestPublisherSubscription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(ColdTestPublisherSubscription<T> s) {
		ColdTestPublisherSubscription<T>[] a = subscribers;

		if (a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == EMPTY) {
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

			ColdTestPublisherSubscription<T>[] b = new ColdTestPublisherSubscription[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	static final class ColdTestPublisherSubscription<T> implements Subscription {

		final Subscriber<? super T>                     actual;
		final Fuseable.ConditionalSubscriber<? super T> actualConditional;

		final ColdTestPublisher<T> parent;

		volatile boolean cancelled;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ColdTestPublisherSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ColdTestPublisherSubscription.class, "requested");

		volatile long wip;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ColdTestPublisherSubscription> WIP =
				AtomicLongFieldUpdater.newUpdater(ColdTestPublisherSubscription.class, "wip");

		/** Where in the {@link ColdTestPublisher#values} buffer this subscription is at. */
		int index;


		@SuppressWarnings("unchecked")
		ColdTestPublisherSubscription(Subscriber<? super T> actual, ColdTestPublisher<T> parent) {
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
				if (Operators.addCap(REQUESTED, this, n) == 0) {
					parent.wasRequested = true;
				}
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				ColdTestPublisher.CANCEL_COUNT.incrementAndGet(parent);
				if (parent.violations.contains(DEFER_CANCELLATION) || parent.violations.contains(REQUEST_OVERFLOW)) {
					return;
				}
				cancelled = true;
				parent.remove(this);
			}
		}

		private void drain() {
			if (WIP.getAndIncrement(this) > 0) {
				return;
			}
			for (; ; ) {
				int i = index;
				// Re-read the volatile 'requested' which could have grown via another thread
				long r = requested;
				int emitted = 0;
				if (cancelled) {
					return;
				}

				// This list can only grow while we're in drain(), so no risk in get(i) being out of bounds
				int s = parent.values.size();
				while (i < s) {
					if (emitted == r && !parent.violations.contains(REQUEST_OVERFLOW)) {
						break;
					}
					T t = parent.values.get(i);
					if (t == null && !parent.violations.contains(ALLOW_NULL)) {
						parent.remove(this);
						actual.onError(new NullPointerException("The " + i + "th element was null"));
						return;
					}

					//emit and increase count, potentially using conditional subscriber
					if (actualConditional != null) {
						//noinspection ConstantConditions
						if (actualConditional.tryOnNext(t)) {
							emitted++;
						}
					} else {
						//noinspection ConstantConditions
						actual.onNext(t);
						emitted++;
					}
					i++;
					if (cancelled) {
						return;
					}
				}

				index = i;
				boolean hasMoreData = i < s;
				boolean hasMoreRequest;
				if (emitted > r) { //we did clearly overflow
					assert parent.violations.contains(REQUEST_OVERFLOW);
					parent.hasOverflown = true;
				}
				//let's update the REQUESTED unless we're in fastpath
				if (r != Long.MAX_VALUE) {
					hasMoreRequest = REQUESTED.addAndGet(this, -emitted) > 0;
				}
				else {
					hasMoreRequest = true;
				}

				//let's exit early if we've transmitted the whole buffer and there's a terminal signal
				if (i == s && emitTerminalSignalIfAny()) {
					return;
				}

				//the only remaining early exit condition is if we're in slowpath and we've emitted
				//all the requested amount but there's still values in the buffer...
				//if the parent is configured to errorOnOverflow then we must terminate
				if (hasMoreData && !hasMoreRequest && parent.errorOnOverflow) {
					parent.remove(this);
					actual.onError(Exceptions.failWithOverflow("Can't deliver value due to lack of requests"));
					return;
				}

				//in all other cases, let's loop again in case of additional work, exit otherwise
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
			}
		}

		/**
		 * Attempt to terminate the subscriber if the publisher was marked as terminated.
		 * Note that if that is not the case, it is important to continue the drain loop
		 * since otherwise no downstream signal is going to be pushed yet we'd exit the loop early.
		 *
		 * @return true if the TestPublisher was terminated, false otherwise
		 */
		private boolean emitTerminalSignalIfAny() {
			if (parent.error == Exceptions.TERMINATED) {
				parent.remove(this);
				actual.onComplete();
				return true;
			}
			if (parent.error != null) {
				parent.remove(this);
				actual.onError(parent.error);
				return true;
			}
			return false;
		}
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
		return Mono.from(this);
	}

	@Override
	public ColdTestPublisher<T> assertMinRequested(long n) {
		ColdTestPublisherSubscription<T>[] subs = subscribers;
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
	public ColdTestPublisher<T> assertMaxRequested(long n) {
		ColdTestPublisherSubscription<T>[] subs = subscribers;
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
	public ColdTestPublisher<T> assertSubscribers() {
		ColdTestPublisherSubscription<T>[] s = subscribers;
		if (s == EMPTY) {
			throw new AssertionError("Expected subscribers");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertSubscribers(int n) {
		int sl = subscribers.length;
		if (sl != n) {
			throw new AssertionError("Expected " + n + " subscribers, got " + sl);
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertNoSubscribers() {
		int sl = subscribers.length;
		if (sl != 0) {
			throw new AssertionError("Expected no subscribers, got " + sl);
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertCancelled() {
		if (cancelCount == 0) {
			throw new AssertionError("Expected at least 1 cancellation");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertCancelled(int n) {
		int cc = cancelCount;
		if (cc != n) {
			throw new AssertionError("Expected " + n + " cancellations, got " + cc);
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertNotCancelled() {
		if (cancelCount != 0) {
			throw new AssertionError("Expected no cancellation");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertRequestOverflow() {
		if (!hasOverflown) {
			throw new AssertionError("Expected some request overflow");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertNoRequestOverflow() {
		if (hasOverflown) {
			throw new AssertionError("Unexpected request overflow");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> next(@Nullable T t) {
		if (!violations.contains(ALLOW_NULL)) {
			Objects.requireNonNull(t, "emitted values must be non-null");
		}

		values.add(t);
		for (ColdTestPublisherSubscription<T> s : subscribers) {
			s.drain();
		}

		return this;
	}

	@Override
	public ColdTestPublisher<T> error(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		ColdTestPublisherSubscription<?>[] subs = subscribers;
		for (ColdTestPublisherSubscription<?> s : subs) {
			s.drain();
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> complete() {
		ColdTestPublisherSubscription<?>[] subs = subscribers;
		error = Exceptions.TERMINATED;
		for (ColdTestPublisherSubscription<?> s : subs) {
			s.drain();
		}
		return this;
	}

}
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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A cold implementation of a {@link TestPublisher}.
 *
 * @author Simon Basle
 * @author Stephane Maldini
 */
final class ColdTestPublisher<T> extends TestPublisher<T> {

	@SuppressWarnings("rawtypes")
	private static final ColdTestPublisherSubscription[] EMPTY = new ColdTestPublisherSubscription[0];

	final boolean            onBackpressureBuffer;
	final boolean            nonCompliant;
	final List<T>            values;

	Throwable     error;

	volatile boolean hasOverflown;

	@SuppressWarnings("unchecked")
	volatile ColdTestPublisherSubscription<T>[] subscribers = EMPTY;

	volatile long requestedTotal;
	static final AtomicLongFieldUpdater<ColdTestPublisher> REQUESTED_TOTAL =
			AtomicLongFieldUpdater.newUpdater(ColdTestPublisher.class, "requestedTotal");

	volatile int cancelCount;
	static final AtomicIntegerFieldUpdater<ColdTestPublisher> CANCEL_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(ColdTestPublisher.class, "cancelCount");

	volatile long subscribeCount;
	static final AtomicLongFieldUpdater<ColdTestPublisher> SUBSCRIBED_COUNT =
			AtomicLongFieldUpdater.newUpdater(ColdTestPublisher.class, "subscribeCount");

	ColdTestPublisher(boolean onBackpressureBuffer, boolean nonCompliant) {
		this.values = Collections.synchronizedList(new ArrayList<>());
		this.onBackpressureBuffer = onBackpressureBuffer;
		this.nonCompliant = nonCompliant;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		ColdTestPublisherSubscription<T> p = new ColdTestPublisherSubscription<>(s, this);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
			ColdTestPublisher.SUBSCRIBED_COUNT.incrementAndGet(this);
			s.onSubscribe(p);
		} else {
			s.onSubscribe(p);
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			} else {
				s.onComplete();
			}
		}
	}

	boolean add(ColdTestPublisherSubscription<T> s) {
		synchronized (this) {

			ColdTestPublisherSubscription<T>[] a = subscribers;
			int len = a.length;

			@SuppressWarnings("unchecked")
			ColdTestPublisherSubscription<T>[] b = new ColdTestPublisherSubscription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
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
		final Context                                   ctx;

		final ColdTestPublisher<T> parent;
		final Queue<T>             values;

		volatile boolean cancelled;

		volatile long requested;
		static final AtomicLongFieldUpdater<ColdTestPublisherSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ColdTestPublisherSubscription.class, "requested");

		volatile int                                                          wip;
		static final AtomicIntegerFieldUpdater<ColdTestPublisherSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ColdTestPublisherSubscription.class, "wip");

		@SuppressWarnings("unchecked")
		ColdTestPublisherSubscription(Subscriber<? super T> actual, ColdTestPublisher<T> parent) {
			this.actual = actual;
			if(actual instanceof Fuseable.ConditionalSubscriber){
				this.actualConditional = (Fuseable.ConditionalSubscriber<? super T>) actual;
			}
			else {
				this.actualConditional = null;
			}
			if (actual instanceof CoreSubscriber) {
				this.ctx = ((CoreSubscriber) actual).currentContext();
			}
			else {
				this.ctx = Context.empty();
			}
			this.parent = parent;
			this.values = Queues.<T>unboundedMultiproducer().get();
			this.values.addAll(parent.values);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED_TOTAL, parent, n);
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			boolean nonCompliant = parent.nonCompliant;
			boolean bufferOnOverflow = parent.onBackpressureBuffer;
			int missed = 1;

			final Queue<T> q = values;
			final Subscriber<? super T> a = actual;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {

					T t = q.poll();
					boolean empty = t == null;

					if (checkTerminated(empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					if(actualConditional != null) {
						if (actualConditional.tryOnNext(t)) {
							e++;
						}
					}
					else {
						a.onNext(t);
						e++;
					}

					if (nonCompliant) {
						if (r == e) {
							parent.hasOverflown = true;
						}
						e = 0L;
					}
				}

				if (r == e) {
					boolean qEmpty = q.isEmpty();
					if (!bufferOnOverflow && !nonCompliant && !qEmpty) {
						parent.remove(this);
						a.onError(new IllegalStateException("Can't deliver value due to lack of requests"));
						return;
					}
					if (checkTerminated(qEmpty, a)) {
						return;
					}
				}

				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed < 1) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean valueCacheEmpty, Subscriber<? super T> a) {
			if (cancelled) {
				Operators.onDiscardQueueWithClear(values, ctx, null);
				return true;
			}
			if (valueCacheEmpty) {
				Throwable e = parent.error;
				if (e == Exceptions.TERMINATED) {
					parent.remove(this);
					a.onComplete();
					return true;
				}
				else if (e != null) {
					parent.remove(this);
					Operators.onDiscardQueueWithClear(values, ctx, null);
					a.onError(e);
					return true;
				}
			}
			return false;
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				ColdTestPublisher.CANCEL_COUNT.incrementAndGet(parent);
				if (parent.nonCompliant) {
					return;
				}
				cancelled = true;
				Operators.onDiscardQueueWithClear(values, ctx, null);
				parent.remove(this);
			}
		}

		void hotNext(T value) {
			values.offer(value);
			drain();
		}

		void hotTermination() {
			drain();
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
		return requestedTotal > 0;
	}

	@Override
	public long requestedTotal() {
		return requestedTotal;
	}

	@Override
	public Mono<T> mono() {
		if (nonCompliant) {
			return Mono.fromDirect(this);
		}
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
			throw new AssertionError("Expected minimum request of " + n + "; got " + minRequest);
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
		//the cold publisher can only overflow requests when next is invoked after subscription,
		// and only if it was created with the onBackpressureBuffer parameter to false
		if (!hasOverflown) {
			throw new AssertionError("Expected some request overflow");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> assertNoRequestOverflow() {
		//the cold publisher can only overflow requests when next is invoked after subscription,
		// and only if it was created with the onBackpressureBuffer parameter to false
		if (hasOverflown) {
			throw new AssertionError("Expected no request overflow");
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> next(@Nullable T t) {
		Objects.requireNonNull(t, "emitted values must be non-null");

		values.add(t);
		for (ColdTestPublisherSubscription<T> s : subscribers) {
			s.hotNext(t);
		}

		return this;
	}

	@Override
	public ColdTestPublisher<T> error(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		ColdTestPublisherSubscription<?>[] subs = subscribers;
		for (ColdTestPublisherSubscription<?> s : subs) {
			s.hotTermination();
		}
		return this;
	}

	@Override
	public ColdTestPublisher<T> complete() {
		ColdTestPublisherSubscription<?>[] subs = subscribers;
		error = Exceptions.TERMINATED;
		for (ColdTestPublisherSubscription<?> s : subs) {
			s.hotTermination();
		}
		return this;
	}

}
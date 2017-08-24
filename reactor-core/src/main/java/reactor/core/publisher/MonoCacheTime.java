/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * An operator that caches the value from a source Mono with a TTL, after which the value
 * expires and the next subscription will trigger a new source subscription.
 *
 * @author Simon Baslé
 */
class MonoCacheTime<T> extends MonoOperator<T, T> {

	private static final Logger LOGGER = Loggers.getLogger(MonoCacheTime.class);

	private final int STATE_INIT = 0;
	private final int STATE_SUBSCRIBED = 1; //no value received
	private final int STATE_CACHED = 2; //values received

	final Duration ttl;
	final Scheduler clock;

	volatile int                      state;
	volatile CoordinatorSubscriber<T> coordinator;
	static final AtomicIntegerFieldUpdater<MonoCacheTime> STATE =
			AtomicIntegerFieldUpdater.newUpdater(MonoCacheTime.class, "state");

	volatile Signal<T> cached;

	MonoCacheTime(Mono<? extends T> source, Duration ttl, Scheduler clock) {
		super(source);
		this.ttl = ttl;
		this.clock = clock;
		this.state = STATE_INIT;
	}

	void expire() {
		LOGGER.debug("expired {}", cached);
		if (STATE.compareAndSet(this, STATE_CACHED, STATE_INIT)) {
			cached = null;
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		//noinspection ConstantConditions
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}

		if (state == STATE_CACHED) {
			s.onSubscribe(new CacheInner<>(s, coordinator, cached));
		}
		else if (STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED)) {
			coordinator = new CoordinatorSubscriber<>(this);
			CacheInner<T> inner = new CacheInner<>(s, coordinator);
			source.subscribe(coordinator);
			s.onSubscribe(inner);
		}
		else {
			//STATE_SUBSCRIBED
			CacheInner<T> inner = new CacheInner<>(s, coordinator);
			s.onSubscribe(inner);
		}
	}

	static final class CacheInner<T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final CoordinatorSubscriber<T>  coordinator;

		volatile Signal<T> cached;
		volatile boolean requested;

		public CacheInner(CoreSubscriber<? super T> actual, CoordinatorSubscriber<T> coordinator,
				@Nullable Signal<T> cached) {
			this.actual = actual;
			this.coordinator = coordinator;
			this.cached = cached;
			coordinator.add(this);
		}

		public CacheInner(CoreSubscriber<? super T> actual, CoordinatorSubscriber<T> coordinator) {
			this(actual, coordinator, null);
		}

		public void setCached(Signal<T> cached) {
			if (requested) {
				cached.accept(actual);
			}
			else {
				this.cached = cached;
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long l) {
			if (cached == null) {
				requested = true;
			}
			else {
				cached.accept(actual);
				if (cached.isOnNext()) {
					actual.onComplete();
				}
			}
		}

		@Override
		public void cancel() {
			coordinator.remove(this);
		}
	}

	static final class CoordinatorSubscriber<T> implements InnerConsumer<T> {

		final MonoCacheTime<T> main;
		final Scheduler.Worker worker;

		Signal<T>    cached;
		Disposable   timer;

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, Subscription.class, "subscription");

		volatile CacheInner<T>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, CacheInner[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, CacheInner[].class, "subscribers");

		public CoordinatorSubscriber(MonoCacheTime<T> main) {
			this.main = main;
			//noinspection unchecked
			this.subscribers = EMPTY;
			this.worker = main.clock.createWorker();
		}

		final boolean add(CacheInner<T> toAdd) {
			for (; ; ) {
				CacheInner<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;
				//noinspection unchecked
				CacheInner<T>[] b = new CacheInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = toAdd;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		final void remove(CacheInner<T> toRemove) {
			for (; ; ) {
				CacheInner<T>[] a = subscribers;
				if (a == TERMINATED || a == EMPTY) {
					return;
				}
				int n = a.length;
				int j = -1;
				for (int i = 0; i < n; i++) {
					if (a[i] == toRemove) {
						j = i;
						break;
					}
				}

				if (j < 0) {
					return;
				}

				CacheInner<?>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new CacheInner<?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					if (b == EMPTY && Operators.terminate(S, this)) {
						main.state = main.STATE_INIT;
					}
				}
				return;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(subscription, s)) {
				subscription = s;
				s.request(Long.MAX_VALUE);
			}
		}

		private void signalCached(Signal<T> signal) {
			if (STATE.compareAndSet(main, main.STATE_SUBSCRIBED, main.STATE_CACHED)) {
				this.cached = signal;
				main.cached = signal;
				timer = worker.schedule(main::expire, main.ttl.toMillis(), TimeUnit.MILLISECONDS);
			}
			else {
				LOGGER.debug("signalCached({}) with unexpected main state {}", signal, main.state);
			}
		}

		@Override
		public void onNext(T t) {
			signalCached(Signal.next(t));
			for (CacheInner<T> csub: subscribers) {
				csub.setCached(cached);
			}
		}

		@Override
		public void onError(Throwable t) {
			signalCached(Signal.error(t));
			for (CacheInner<T> csub: subscribers) {
				csub.setCached(cached);
			}
		}

		@Override
		public void onComplete() {
			if (cached == null) {
				signalCached(Signal.complete());
				for (CacheInner<T> csub: subscribers) {
					csub.setCached(cached);
				}
			}
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		private static final CacheInner[] TERMINATED = new CacheInner[0];
		private static final CacheInner[] EMPTY = new CacheInner[0];
	}

}

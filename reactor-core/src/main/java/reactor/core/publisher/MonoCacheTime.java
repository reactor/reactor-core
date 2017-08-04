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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxReplay.SizeAndTimeBoundReplayBuffer;
import reactor.core.scheduler.Scheduler;

import static reactor.core.publisher.FluxReplay.ReplaySubscriber.EMPTY;
import static reactor.core.publisher.FluxReplay.ReplaySubscriber.TERMINATED;

/**
 * An operator that caches the value from a source Mono with a TTL, after which the value
 * expires and the next subscription will trigger a new source subscription.
 *
 * @author Simon Baslé
 */
class MonoCacheTime<T> extends MonoOperator<T, T> {

	final Duration ttl;
	final Scheduler clock;

	SizeAndTimeBoundReplayBuffer<T> buffer;

	volatile FluxReplay.ReplaySubscription<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<MonoCacheTime, FluxReplay.ReplaySubscription[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheTime.class, FluxReplay.ReplaySubscription[].class, "subscribers");

	MonoCacheTime(Mono<? extends T> source, Duration ttl, Scheduler clock) {
		super(source);
		this.ttl = ttl;
		this.clock = clock;
		this.buffer = null;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		if (buffer == null || buffer.isExpired()) {
			buffer = new SizeAndTimeBoundReplayBuffer<>(1, ttl.toMillis(), clock);
			source.subscribe(new SourceSubscriber<>(this));
		}
		//noinspection ConstantConditions
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		FluxReplay.ReplaySubscription<T> rs = new ReplayInner<>(s, this);
		s.onSubscribe(rs);

		if (add(rs)) {
			if (rs.isCancelled()) {
				remove(rs);
				return;
			}
		}
		buffer.replay(rs);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.CAPACITY) return 1;

		return super.scanUnsafe(key);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	boolean add(FluxReplay.ReplaySubscription<T> rs) {
		for (; ; ) {
			FluxReplay.ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int n = a.length;

			@SuppressWarnings("unchecked") FluxReplay.ReplaySubscription<T>[] b =
					new ReplayInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = rs;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(FluxReplay.ReplaySubscription<T> rs) {
		outer:
		for (; ; ) {
			FluxReplay.ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int n = a.length;

			for (int i = 0; i < n; i++) {
				if (a[i] == rs) {
					FluxReplay.ReplaySubscription<T>[] b;

					if (n == 1) {
						b = EMPTY;
					}
					else {
						b = new ReplayInner[n - 1];
						System.arraycopy(a, 0, b, 0, i);
						System.arraycopy(a, i + 1, b, i, n - i - 1);
					}

					if (SUBSCRIBERS.compareAndSet(this, a, b)) {
						return;
					}

					continue outer;
				}
			}

			break;
		}
	}

	static final class ReplayInner<T> implements FluxReplay.ReplaySubscription<T> {

		final CoreSubscriber<? super T> actual;

		final MonoCacheTime<T> parent;

		final FluxReplay.ReplayBuffer<T> buffer;

		int index;

		int tailIndex;

		Object node;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplayInner> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplayInner.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReplayInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ReplayInner.class, "requested");

		volatile boolean cancelled;

		int fusionMode;

		ReplayInner(CoreSubscriber<? super T> actual, MonoCacheTime<T> parent) {
			this.actual = actual;
			this.parent = parent;
			this.buffer = parent.buffer;
		}

		@Override
		public long requested() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				fusionMode = Fuseable.ASYNC;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			return buffer.poll(this);
		}

		@Override
		public void clear() {
			buffer.clear(this);
		}

		@Override
		public boolean isEmpty() {
			return buffer.isEmpty(this);
		}

		@Override
		public int size() {
			return buffer.size(this);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (fusionMode() == Fuseable.NONE) {
					Operators.getAndAddCap(REQUESTED, this, n);
				}
				buffer.replay(this);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				parent.remove(this);

				if (enter()) {
					node = null;
				}
			}
		}

		@Override
		public void node(@Nullable Object node) {
			this.node = node;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		@Nullable
		public Object node() {
			return node;
		}

		@Override
		public int index() {
			return index;
		}

		@Override
		public void index(int index) {
			this.index = index;
		}

		@Override
		public int tailIndex() {
			return tailIndex;
		}

		@Override
		public void tailIndex(int tailIndex) {
			this.tailIndex = tailIndex;
		}

		@Override
		public boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}

		@Override
		public int leave(int missed) {
			return WIP.addAndGet(this, -missed);
		}

		@Override
		public void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}

	static final class SourceSubscriber<T> implements InnerConsumer<T> {

		final MonoCacheTime<T> main;
		Subscription subscription;

		public SourceSubscriber(MonoCacheTime<T> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (main.buffer.isDone()) {
				s.cancel();
			}
			else if (Operators.validate(subscription, s)) {
				subscription = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			FluxReplay.ReplayBuffer<T> b = main.buffer;
			if (b.isDone()) {
				Operators.onNextDropped(t);
			}
			else {
				b.add(t);
				for (FluxReplay.ReplaySubscription<T> rs : main.subscribers) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			FluxReplay.ReplayBuffer<T> b = main.buffer;
			if (b.isDone()) {
				Operators.onErrorDropped(t);
			}
			else {
				b.onError(t);

				@SuppressWarnings("unchecked")
				FluxReplay.ReplaySubscription<T>[] a = SUBSCRIBERS.getAndSet(main, TERMINATED);

				for (FluxReplay.ReplaySubscription<T> rs : a) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onComplete() {
			FluxReplay.ReplayBuffer<T> b = main.buffer;
			if (!b.isDone()) {
				b.onComplete();

				@SuppressWarnings("unchecked") FluxReplay.ReplaySubscription<T>[] a =
						SUBSCRIBERS.getAndSet(main, TERMINATED);

				for (FluxReplay.ReplaySubscription<T> rs : a) {
					b.replay(rs);
				}
			}
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT){
				return subscription;
			}
			return null;
		}
	}

}

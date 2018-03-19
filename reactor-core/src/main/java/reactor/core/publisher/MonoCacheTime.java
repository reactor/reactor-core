/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * An operator that caches the value from a source Mono with a TTL, after which the value
 * expires and the next subscription will trigger a new source subscription.
 *
 * @author Simon Baslé
 */
class MonoCacheTime<T> extends MonoOperator<T, T> implements Runnable {

	private static final Logger LOGGER = Loggers.getLogger(MonoCacheTime.class);

	final Function<? super Signal<T>, Duration> ttlGenerator;
	final Scheduler                             clock;

	volatile Signal<T> state;
	static final AtomicReferenceFieldUpdater<MonoCacheTime, Signal> STATE =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheTime.class, Signal.class, "state");

	static final Signal<?> EMPTY = new ImmutableSignal<>(Context.empty(), SignalType.ON_NEXT, null, null, null);

	MonoCacheTime(Mono<? extends T> source, Duration ttl, Scheduler clock) {
		super(source);
		this.ttlGenerator = ignoredSignal -> ttl;
		this.clock = clock;
		//noinspection unchecked
		this.state = (Signal<T>) EMPTY;
	}

	MonoCacheTime(Mono<? extends T> source, Function<? super Signal<T>, Duration> ttlGenerator,
			Scheduler clock) {
		super(source);
		this.ttlGenerator = ttlGenerator;
		this.clock = clock;
		//noinspection unchecked
		this.state = (Signal<T>) EMPTY;
	}

	MonoCacheTime(Mono<? extends T> source,
			Function<? super T, Duration> valueTtlGenerator,
			Function<Throwable, Duration> errorTtlGenerator,
			Supplier<Duration> emptyTtlGenerator,
			Scheduler clock) {
		super(source);
		this.ttlGenerator = sig -> {
			if (sig.isOnNext()) return valueTtlGenerator.apply(sig.get());
			if (sig.isOnError()) return errorTtlGenerator.apply(sig.getThrowable());
			return emptyTtlGenerator.get();
		};
		this.clock = clock;
		@SuppressWarnings("unchecked")
		Signal<T> emptyState = (Signal<T>) EMPTY;
		this.state = emptyState;
	}

	public void run() {
		LOGGER.debug("expired {}", state);
		@SuppressWarnings("unchecked")
		Signal<T> emptyState = (Signal<T>) EMPTY;
		state = emptyState;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		for(;;){
			Signal<T> state = this.state;
			if (state == EMPTY) {
				//init or expired
				CoordinatorSubscriber<T> newState = new CoordinatorSubscriber<>(this);
				if (STATE.compareAndSet(this, EMPTY, newState)) {
					CacheMonoSubscriber<T> inner = new CacheMonoSubscriber<>(actual, newState);
					if (newState.add(inner)) {
						actual.onSubscribe(inner);
						source.subscribe(newState);
						break;
					}
				}
			}
			else if (state instanceof CoordinatorSubscriber) {
				//subscribed to source once, but not yet valued / cached
				CoordinatorSubscriber<T> coordinator = (CoordinatorSubscriber<T>) state;

				CacheMonoSubscriber<T> inner = new CacheMonoSubscriber<>(actual, coordinator);
				if (coordinator.add(inner)) {
					actual.onSubscribe(inner);
					break;
				}
			}
			else {
				//state is an actual signal, cached
				if (state.isOnNext()) {
					actual.onSubscribe(new Operators.ScalarSubscription<>(actual, state.get()));
				}
				else if (state.isOnComplete()) {
					Operators.complete(actual);
				}
				else {
					Operators.error(actual, state.getThrowable());
				}
				break;
			}
		}
	}

	static final class CoordinatorSubscriber<T> implements InnerConsumer<T>, Signal<T> {

		final MonoCacheTime<T> main;

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, Subscription.class, "subscription");

		volatile Operators.MonoSubscriber<T, T>[] subscribers;
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, Operators.MonoSubscriber[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, Operators.MonoSubscriber[].class, "subscribers");

		@SuppressWarnings("unchecked")
		CoordinatorSubscriber(MonoCacheTime<T> main) {
			this.main = main;
			this.subscribers = EMPTY;
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public Throwable getThrowable() {
			throw new UnsupportedOperationException("illegal signal use");
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public Subscription getSubscription() {
			throw new UnsupportedOperationException("illegal signal use");
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public T get() {
			throw new UnsupportedOperationException("illegal signal use");
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public SignalType getType() {
			throw new UnsupportedOperationException("illegal signal use");
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public Context getContext() {
			throw new UnsupportedOperationException("illegal signal use: getContext");
		}

		final boolean add(Operators.MonoSubscriber<T, T> toAdd) {
			for (; ; ) {
				Operators.MonoSubscriber<T, T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;
				@SuppressWarnings("unchecked")
				Operators.MonoSubscriber<T, T>[] b = new Operators.MonoSubscriber[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = toAdd;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		final void remove(Operators.MonoSubscriber<T, T> toRemove) {
			for (; ; ) {
				Operators.MonoSubscriber<T, T>[] a = subscribers;
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

				Operators.MonoSubscriber<?, ?>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new Operators.MonoSubscriber<?, ?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					//no particular cleanup here for the EMPTY case, we don't cancel the
					// source because a new subscriber could come in before the coordinator
					// is terminated.

					return;
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(subscription, s)) {
				subscription = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@SuppressWarnings("unchecked")
		private void signalCached(Signal<T> signal) {
			Signal<T> signalToPropagate = signal;
			if (STATE.compareAndSet(main, this, signal)) {
				Duration ttl = null;
				try {
					ttl = main.ttlGenerator.apply(signal);
				}
				catch (Throwable generatorError) {
					signalToPropagate = Signal.error(generatorError);
					STATE.set(main, signalToPropagate);
					if (signal.isOnError()) {
						//noinspection ThrowableNotThrown
						Exceptions.addSuppressed(generatorError, signal.getThrowable());
					}
				}

				if (ttl != null) {
					main.clock.schedule(main, ttl.toMillis(), TimeUnit.MILLISECONDS);
				}
				else {
					//error during TTL generation, signal != updatedSignal, aka dropped
					if (signal.isOnNext()) {
						Operators.onNextDropped(signal.get(), currentContext());
					}
					else if (signal.isOnError()) {
						Operators.onErrorDropped(signal.getThrowable(), currentContext());
					}
					//immediate cache clear
					main.run();
				}
			}

			for (Operators.MonoSubscriber<T, T> inner : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
				if (signalToPropagate.isOnNext()) {
					inner.complete(signalToPropagate.get());
				}
				else if (signalToPropagate.isOnError()) {
					inner.onError(signalToPropagate.getThrowable());
				}
				else {
					inner.onComplete();
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (main.state != this) {
				Operators.onNextDroppedMulticast(t);
				return;
			}
			signalCached(Signal.next(t));
		}

		@Override
		public void onError(Throwable t) {
			if (main.state != this) {
				Operators.onErrorDroppedMulticast(t);
				return;
			}
			signalCached(Signal.error(t));
		}

		@Override
		public void onComplete() {
			signalCached(Signal.complete());
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		private static final Operators.MonoSubscriber[] TERMINATED = new Operators.MonoSubscriber[0];
		private static final Operators.MonoSubscriber[] EMPTY = new Operators.MonoSubscriber[0];
	}

	static final class CacheMonoSubscriber<T> extends Operators.MonoSubscriber<T, T> {

		final CoordinatorSubscriber<T> coordinator;

		CacheMonoSubscriber(CoreSubscriber<? super T> actual, CoordinatorSubscriber<T> coordinator) {
			super(actual);
			this.coordinator = coordinator;
		}

		@Override
		public void cancel() {
			super.cancel();
			coordinator.remove(this);
		}
	}

}

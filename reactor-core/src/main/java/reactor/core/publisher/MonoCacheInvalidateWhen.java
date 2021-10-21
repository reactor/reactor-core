/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.MonoCacheInvalidateIf.State;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.publisher.MonoCacheInvalidateIf.EMPTY_STATE;

/**
 * A caching operator that uses a companion {@link Mono} as a trigger to invalidate the cached value.
 * Subscribers accumulated between the upstream subscription is triggered and the actual value is received and stored
 * don't get impacted by the generated trigger and will immediately receive the cached value.
 * If after that the trigger completes immediately, the next incoming subscriber will lead to a resubscription to the
 * source and a new caching cycle.
 *
 * @author Simon Baslé
 */
final class MonoCacheInvalidateWhen<T> extends InternalMonoOperator<T, T> {

	private static final Logger LOGGER = Loggers.getLogger(MonoCacheInvalidateWhen.class);

	final Function<? super T, Mono<Void>> invalidationTriggerGenerator;
	@Nullable
	final Consumer<? super T>             invalidateHandler;

	volatile     State<T>   state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<MonoCacheInvalidateWhen, State> STATE =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheInvalidateWhen.class, State.class, "state");

	MonoCacheInvalidateWhen(Mono<T> source, Function<? super T, Mono<Void>> invalidationTriggerGenerator,
	                        @Nullable Consumer<? super T> invalidateHandler) {
		super(source);
		this.invalidationTriggerGenerator = Objects.requireNonNull(invalidationTriggerGenerator, "invalidationTriggerGenerator");
		this.invalidateHandler = invalidateHandler;
		@SuppressWarnings("unchecked")
		State<T> state = (State<T>) EMPTY_STATE; //from MonoCacheInvalidateIf
		this.state = state;
	}

	boolean compareAndInvalidate(State<T> expected) {
		if (STATE.compareAndSet(this, expected, EMPTY_STATE)) {
			if (expected instanceof MonoCacheInvalidateIf.ValueState) {
				LOGGER.trace("invalidated {}", expected.get());
				safeInvalidateHandler(expected.get());
			}
			return true;
		}
		return false;
	}

	void invalidate() {
		@SuppressWarnings("unchecked")
		State<T> oldState = STATE.getAndSet(this, EMPTY_STATE);
		if (oldState instanceof MonoCacheInvalidateIf.ValueState) {
			LOGGER.trace("invalidated {}", oldState.get());
			safeInvalidateHandler(oldState.get());
		}
	}

	void safeInvalidateHandler(@Nullable T value) {
		if (value != null && this.invalidateHandler != null) {
			try {
				this.invalidateHandler.accept(value);
			}
			catch (Throwable invalidateHandlerError) {
				LOGGER.warnOrDebug(
					isVerbose -> isVerbose ? "Failed to apply invalidate handler on value " + value : "Failed to apply invalidate handler",
					invalidateHandlerError);
			}
		}
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		CacheMonoSubscriber<T> inner = new CacheMonoSubscriber<>(actual);
		actual.onSubscribe(inner);
		for(;;) {
			State<T> state = this.state;
			if (state == EMPTY_STATE || state instanceof CoordinatorSubscriber) {
				boolean subscribe = false;
				CoordinatorSubscriber<T> coordinator;
				if (state == EMPTY_STATE) {
					coordinator = new CoordinatorSubscriber<>(this);
					if (!STATE.compareAndSet(this, EMPTY_STATE, coordinator)) {
						continue;
					}
					subscribe = true;
				}
				else {
					coordinator = (CoordinatorSubscriber<T>) state;
				}

				if (coordinator.add(inner)) {
					if (inner.isCancelled()) {
						coordinator.remove(inner);
					}
					else {
						inner.coordinator = coordinator;
					}

					if (subscribe) {
						source.subscribe(coordinator);
					}
					return null;
				}
			}
			else {
				//state is an actual signal, cached
				inner.complete(state.get());
				return null;
			}
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class CoordinatorSubscriber<T> implements InnerConsumer<T>, State<T> {

		final MonoCacheInvalidateWhen<T> main;

		Subscription subscription;

		volatile     CacheMonoSubscriber<T>[]                                                  subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, CacheMonoSubscriber[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, CacheMonoSubscriber[].class, "subscribers");

		@SuppressWarnings("rawtypes")
		private static final CacheMonoSubscriber[] COORDINATOR_DONE = new CacheMonoSubscriber[0];
		@SuppressWarnings("rawtypes")
		private static final CacheMonoSubscriber[] COORDINATOR_INIT = new CacheMonoSubscriber[0];

		@SuppressWarnings("unchecked")
		CoordinatorSubscriber(MonoCacheInvalidateWhen<T> main) {
			this.main = main;
			this.subscribers = COORDINATOR_INIT;
		}

		/**
		 * unused in this context as the {@link State} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Nullable
		@Override
		public T get() {
			throw new UnsupportedOperationException("coordinator State#get shouldn't be used");
		}

		/**
		 * unused in this context as the {@link State} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public void clear() {
			//NO-OP
		}

		final boolean add(CacheMonoSubscriber<T> toAdd) {
			for (; ; ) {
				CacheMonoSubscriber<T>[] a = subscribers;
				if (a == COORDINATOR_DONE) {
					return false;
				}
				int n = a.length;
				@SuppressWarnings("unchecked")
				CacheMonoSubscriber<T>[] b = new CacheMonoSubscriber[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = toAdd;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		final void remove(CacheMonoSubscriber<T> toRemove) {
			for (; ; ) {
				CacheMonoSubscriber<T>[] a = subscribers;
				if (a == COORDINATOR_DONE || a == COORDINATOR_INIT) {
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

				if (n == 1) {
					if (SUBSCRIBERS.compareAndSet(this, a, COORDINATOR_DONE)) {
						if (main.compareAndInvalidate(this)) {
							this.subscription.cancel();
						}
						return;
					}
					//else loop back
				}
				else {
					CacheMonoSubscriber<?>[] b = new CacheMonoSubscriber<?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
					if (SUBSCRIBERS.compareAndSet(this, a, b)) {
						return;
					}
					//else loop back
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.subscription, s)) {
				this.subscription = s;
				s.request(Long.MAX_VALUE);
			}
		}

		boolean cacheLoadFailure(State<T> expected, Throwable failure) {
			if (STATE.compareAndSet(main, expected, EMPTY_STATE)) {
				for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
					inner.onError(failure);
				}
				//no need to invalidate, EMPTY_STATE swap above is equivalent for our purpose
				return true;
			}
			return false;
		}

		void cacheLoad(T value) {
			State<T> valueState = new MonoCacheInvalidateIf.ValueState<>(value);
			if (STATE.compareAndSet(main, this, valueState)) {
				Mono<Void> invalidateTrigger = null;
				try {
					invalidateTrigger = Objects.requireNonNull(
							main.invalidationTriggerGenerator.apply(value),
							"invalidationTriggerGenerator produced a null trigger");
				}
				catch (Throwable generatorError) {
					if (cacheLoadFailure(valueState, generatorError)) {
						main.safeInvalidateHandler(value);
					}
					return;
				}

				for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
					inner.complete(value);
				}
				invalidateTrigger.subscribe(new TriggerSubscriber(this.main));
			}
		}

		@Override
		public void onNext(T t) {
			if (main.state != this) {
				Operators.onNextDroppedMulticast(t, subscribers);
				return;
			}
			cacheLoad(t);
		}

		@Override
		public void onError(Throwable t) {
			if (main.state != this) {
				Operators.onErrorDroppedMulticast(t, subscribers);
				return;
			}
			cacheLoadFailure(this, t);
		}

		@Override
		public void onComplete() {
			if (main.state == this) {
				//completed empty
				cacheLoadFailure(this, new NoSuchElementException("cacheInvalidateWhen expects a value, source completed empty"));
			}
			//otherwise, main.state MUST be a ValueState at this point
		}

		@Override
		public Context currentContext() {
			return Operators.multiSubscribersContext(subscribers);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return null;
		}

	}

	static final class CacheMonoSubscriber<T> extends Operators.MonoSubscriber<T, T> {

		CoordinatorSubscriber<T> coordinator;

		CacheMonoSubscriber(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public void cancel() {
			super.cancel();
			CoordinatorSubscriber<T> coordinator = this.coordinator;
			if (coordinator != null) {
				coordinator.remove(this);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return coordinator.main;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}
	}


	static final class TriggerSubscriber implements InnerConsumer<Void> {

		final MonoCacheInvalidateWhen<?> main;

		TriggerSubscriber(MonoCacheInvalidateWhen<?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(1);
		}

		@Override
		public void onNext(Void unused) {
			//NO-OP
		}

		@Override
		public void onError(Throwable t) {
			LOGGER.debug("Invalidation triggered by onError(" + t + ")");
			main.invalidate();
		}

		@Override
		public void onComplete() {
			main.invalidate();
		}

		@Override
		public Context currentContext() {
			return Context.empty();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main;
			//this is an approximation of TERMINATED, the trigger should only be active AFTER an actual value has been set as STATE
			if (key == Attr.TERMINATED) return main.state == EMPTY_STATE || main.state instanceof CoordinatorSubscriber;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PREFETCH) return 1;
			return null;
		}
	}
}

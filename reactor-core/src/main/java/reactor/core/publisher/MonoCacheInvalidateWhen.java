/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * @author Simon Baslé
 */
final class MonoCacheInvalidateWhen<T> extends InternalMonoOperator<T, T> {

	private static final Logger LOGGER = Loggers.getLogger(MonoCacheInvalidateWhen.class);

	final Function<? super T, Mono<Void>> invalidationTriggerGenerator;
	@Nullable
	final Consumer<? super T>             invalidateHandler;

	volatile     Signal<T>                                                    state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<MonoCacheInvalidateWhen, Signal> STATE =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheInvalidateWhen.class, Signal.class, "state");

	static final Signal<?> EMPTY_STATE = new ImmutableSignal<>(Context.empty(), SignalType.ON_NEXT, null, null, null);

	MonoCacheInvalidateWhen(Mono<T> source, Function<? super T, Mono<Void>> invalidationTriggerGenerator,
	                        @Nullable Consumer<? super T> invalidateHandler) {
		super(source);
		this.invalidationTriggerGenerator = Objects.requireNonNull(invalidationTriggerGenerator, "invalidationTriggerGenerator");
		this.invalidateHandler = invalidateHandler;
		@SuppressWarnings("unchecked")
		Signal<T> state = (Signal<T>) EMPTY_STATE;
		this.state = state;
	}

	boolean invalidate(Signal<?> expected) {
		if (STATE.compareAndSet(this, expected, EMPTY_STATE)) {
			LOGGER.trace("invalidated {}", expected);
			if (expected.isOnNext() && this.invalidateHandler != null) {
				//noinspection unchecked
				invalidateHandler.accept((T) expected.get());
			}
			return true;
		}
		return false;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		CacheMonoSubscriber<T> inner = new CacheMonoSubscriber<>(actual);
		actual.onSubscribe(inner);
		for(;;) {
			Signal<T> state = this.state;
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
			else if (state.isOnNext()) {
				//state is an actual signal, cached
				inner.complete(state.get());
				return null;
			}
			// else cached onError, means the state is about to be switched back to EMPTY => continue
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class CoordinatorSubscriber<T> implements InnerConsumer<T>, Signal<T> {

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
		@Nullable
		@Override
		public T get() {
			return null;
		}

		/**
		 * Use of {@link SignalType#AFTER_TERMINATE} ensures that the isXxx methods
		 * used by this operator (isOnNext, isOnError, isOnComplete) will all return
		 * false for this class.
		 */
		@Override
		public SignalType getType() {
			return SignalType.AFTER_TERMINATE;
		}

		/**
		 * unused in this context as the {@link Signal} interface is only
		 * implemented for use in the main's STATE compareAndSet.
		 */
		@Override
		public ContextView getContextView() {
			throw new UnsupportedOperationException("illegal signal use: getContextView");
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
						if (main.invalidate(this)) {
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

		void signalCached(Signal<T> signal) {
			boolean invalidateAtEnd = false;
			Signal<T> signalToPropagate = signal;
			if (STATE.compareAndSet(main, this, signal)) {
				if (signal.get() != null) {
					Mono<Void> invalidateTrigger = null;
					try {
						invalidateTrigger = Objects.requireNonNull(
								main.invalidationTriggerGenerator.apply(signal.get()),
								"invalidationTriggerGenerator produced a null trigger");

						invalidateTrigger.subscribe(new TriggerSubscriber(this.main));
					}
					catch (Throwable generatorError) {
						signalToPropagate = Signal.error(generatorError);
						if (STATE.compareAndSet(main, signal, signalToPropagate)) {
							invalidateAtEnd = true;
							Operators.onDiscard(signal.get(), currentContext());
						}
					}
				}
				else {
					invalidateAtEnd = true;
					if (signal.isOnComplete()) {
						signalToPropagate = Signal.error(new NoSuchElementException("cacheInvalidateWhen expects a value, source completed empty"));
						invalidateAtEnd = STATE.compareAndSet(main, signal, signalToPropagate);
					}
				}

				for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
					if (signalToPropagate.isOnNext()) {
						inner.complete(signalToPropagate.get());
					}
					else if (signalToPropagate.getThrowable() != null) {
						inner.onError(signalToPropagate.getThrowable());
					}
					else {
						inner.onError(new IllegalStateException("unexpected signal cached " + signalToPropagate));
					}
				}
				if (invalidateAtEnd) {
					main.invalidate(signalToPropagate);
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (main.state != this) {
				Operators.onNextDroppedMulticast(t, subscribers);
				return;
			}
			signalCached(Signal.next(t));
		}

		@Override
		public void onError(Throwable t) {
			if (main.state != this) {
				Operators.onErrorDroppedMulticast(t, subscribers);
				return;
			}
			signalCached(Signal.error(t));
		}

		@Override
		public void onComplete() {
			signalCached(Signal.complete());
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
			LOGGER.warn("Invalidation triggered by onError(" + t + ")");
			main.invalidate(main.state);
		}

		@Override
		public void onComplete() {
			main.invalidate(main.state);
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

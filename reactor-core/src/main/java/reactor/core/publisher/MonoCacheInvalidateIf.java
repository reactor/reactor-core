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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class MonoCacheInvalidateIf<T> extends MonoCacheTime<T> {

	private static final Logger LOGGER = Loggers.getLogger(MonoCacheInvalidateIf.class);

	static interface State<T> {

		@Nullable
		T get();

		void clear();
	}

	static final class ValueState<T> implements State<T> {

		@Nullable
		T value;

		ValueState(T value) {
			this.value = value;
		}

		@Nullable
		@Override
		public T get() {
			return value;
		}

		@Override
		public void clear() {
			this.value = null;
		}
	}
	
	final Predicate<? super T> validatingPredicate;
	
	volatile     State<T>                                                    state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<MonoCacheInvalidateIf, State> STATE =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheInvalidateIf.class, State.class, "state");
	
	static final State<?> EMPTY_STATE = new State<Object>() {
		@Nullable
		@Override
		public Object get() {
			return null;
		}

		@Override
		public void clear() {
			//NO-OP
		}
	};

	MonoCacheInvalidateIf(Mono<T> source, Predicate<? super T> validatingPredicate) {
		super(source);
		this.validatingPredicate = validatingPredicate;
		@SuppressWarnings("unchecked")
		State<T> state = (State<T>) EMPTY_STATE;
		this.state = state;
	}


//	boolean invalidate(Signal<?> expected) {
//		if (STATE.compareAndSet(this, expected, EMPTY_STATE)) {
//			LOGGER.trace("invalidated {}", expected);
//			if (expected.isOnNext() && this.invalidateHandler != null) {
//				//noinspection unchecked
//				invalidateHandler.accept((T) expected.get());
//			}
//			return true;
//		}
//		return false;
//	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		CacheMonoSubscriber<T> inner = new CacheMonoSubscriber<>(actual);
		//important: onSubscribe should be deferred until we're sure we're in the Coordinator case OR the cached value passes the predicate
//		actual.onSubscribe(inner);
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
						actual.onSubscribe(inner);
					}

					if (subscribe) {
						source.subscribe(coordinator);
					}
					return null;
				}
			}
			else {
				//state is an actual signal, cached
				actual.onSubscribe(inner);
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

		final MonoCacheInvalidateIf<T> main;

		Subscription subscription;

		volatile     CacheMonoSubscriber<T>[]                                                                         subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CoordinatorSubscriber, CacheMonoSubscriber[]>SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(CoordinatorSubscriber.class, CacheMonoSubscriber[].class, "subscribers");

		@SuppressWarnings("rawtypes")
		private static final CacheMonoSubscriber[] COORDINATOR_DONE = new CacheMonoSubscriber[0];
		@SuppressWarnings("rawtypes")
		private static final CacheMonoSubscriber[] COORDINATOR_INIT = new CacheMonoSubscriber[0];

		@SuppressWarnings("unchecked")
		CoordinatorSubscriber(MonoCacheInvalidateIf<T> main) {
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
						//cancel the subscription no matter what, at this point coordinator is done and cannot accept new subscribers
						this.subscription.cancel();
						//only switch to EMPTY_STATE if the current state is this coordinator
						STATE.compareAndSet(this.main, this, EMPTY_STATE);
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
			//this only happens if there was at least one incoming subscriber
			if (Operators.validate(this.subscription, s)) {
				this.subscription = s;
				s.request(Long.MAX_VALUE);
			}
		}

		volatile boolean gotOnNext = false;

		@Override
		public void onNext(T t) {
			if (main.state != this || gotOnNext) {
				Operators.onNextDropped(t, currentContext());
				return;
			}
			gotOnNext = true;
			boolean invalid = main.validatingPredicate.test(t);
			if (invalid) {
				//resubscribe
			}
			else {
				State<T> valueState = new ValueState<>(t);
				if (STATE.compareAndSet(main, this, valueState)) {
					for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
						inner.complete(t);
					}
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (main.state != this || gotOnNext) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}
			if (STATE.compareAndSet(main, this, EMPTY_STATE)) {
				for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
					inner.onError(t);
				}
			}
		}

		@Override
		public void onComplete() {
			if (gotOnNext) {
				gotOnNext = false;
				//FIXME should the resubscribe happen here?
				return;
			}
			if (STATE.compareAndSet(main, this, EMPTY_STATE)) {
				for (@SuppressWarnings("unchecked") CacheMonoSubscriber<T> inner : SUBSCRIBERS.getAndSet(this, COORDINATOR_DONE)) {
					inner.onError(new NoSuchElementException("cacheInvalidateWhen expects a value, source completed empty"));
				}
			}
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
}

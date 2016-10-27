/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.Cancellation;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;

/**
 * Executes a Callable and emits its value on the given Scheduler.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSubscribeOnCallable<T> extends Flux<T> implements Fuseable {

	final Callable<? extends T> callable;

	final Scheduler scheduler;

	public FluxSubscribeOnCallable(Callable<? extends T> callable, Scheduler scheduler) {
		this.callable = Objects.requireNonNull(callable, "callable");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		subscribe(callable, s, scheduler);
	}

	public static <T> void subscribe(Callable<T> callable,
			Subscriber<? super T> s,
			Scheduler scheduler) {
		CallableSubscribeOnSubscription<T> parent =
				new CallableSubscribeOnSubscription<>(s, callable, scheduler);
		s.onSubscribe(parent);

		Cancellation f = scheduler.schedule(parent);

		parent.setMainFuture(f);
	}

	static final class CallableSubscribeOnSubscription<T>
			implements QueueSubscription<T>, Runnable {

		final Subscriber<? super T> actual;

		final Callable<? extends T> callable;

		final Scheduler scheduler;

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CallableSubscribeOnSubscription> STATE =
				AtomicIntegerFieldUpdater.newUpdater(CallableSubscribeOnSubscription.class,
						"state");

		T value;
		static final int NO_REQUEST_NO_VALUE   = 0;
		static final int NO_REQUEST_HAS_VALUE  = 1;
		static final int HAS_REQUEST_NO_VALUE  = 2;
		static final int HAS_REQUEST_HAS_VALUE = 3;
		static final int CANCELLED             = 4;

		int fusionState;

		static final int NOT_FUSED = 0;
		static final int NO_VALUE  = 1;
		static final int HAS_VALUE = 2;
		static final int COMPLETE  = 3;

		volatile Cancellation mainFuture;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Cancellation>
				MAIN_FUTURE = AtomicReferenceFieldUpdater.newUpdater(
				CallableSubscribeOnSubscription.class,
				Cancellation.class,
				"mainFuture");

		volatile Cancellation requestFuture;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Cancellation>
				REQUEST_FUTURE = AtomicReferenceFieldUpdater.newUpdater(
				CallableSubscribeOnSubscription.class,
				Cancellation.class,
				"requestFuture");

		static final Cancellation CANCEL = () -> {
		};

		public CallableSubscribeOnSubscription(Subscriber<? super T> actual,
				Callable<? extends T> callable,
				Scheduler scheduler) {
			this.actual = actual;
			this.callable = callable;
			this.scheduler = scheduler;
		}

		@Override
		public void cancel() {
			state = CANCELLED;
			fusionState = COMPLETE;
			Cancellation a = mainFuture;
			if (a != CANCEL) {
				a = MAIN_FUTURE.getAndSet(this, CANCEL);
				if (a != null && a != CANCEL) {
					a.dispose();
				}
			}
			a = requestFuture;
			if (a != CANCEL) {
				a = REQUEST_FUTURE.getAndSet(this, CANCEL);
				if (a != null && a != CANCEL) {
					a.dispose();
				}
			}
		}

		@Override
		public void clear() {
			value = null;
			fusionState = COMPLETE;
		}

		@Override
		public boolean isEmpty() {
			return fusionState != HAS_VALUE;
		}

		@Override
		public T poll() {
			if (fusionState == HAS_VALUE) {
				fusionState = COMPLETE;
				return value;
			}
			return null;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0 && (requestedMode & THREAD_BARRIER) == 0) {
				fusionState = NO_VALUE;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		void setMainFuture(Cancellation c) {
			for (; ; ) {
				Cancellation a = mainFuture;
				if (a == CANCEL) {
					c.dispose();
					return;
				}
				if (MAIN_FUTURE.compareAndSet(this, a, c)) {
					return;
				}
			}
		}

		void setRequestFuture(Cancellation c) {
			for (; ; ) {
				Cancellation a = requestFuture;
				if (a == CANCEL) {
					c.dispose();
					return;
				}
				if (REQUEST_FUTURE.compareAndSet(this, a, c)) {
					return;
				}
			}
		}

		@Override
		public void run() {
			T v;

			try {
				v = callable.call();
			}
			catch (Throwable ex) {
				actual.onError(Operators.onOperatorError(this, ex));
				return;
			}

			for (; ; ) {
				int s = state;
				if (s == CANCELLED || s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
					return;
				}
				if (s == HAS_REQUEST_NO_VALUE) {
					if (fusionState == NO_VALUE) {
						this.value = v;
						this.fusionState = HAS_VALUE;
					}
					actual.onNext(v);
					if (state != CANCELLED) {
						actual.onComplete();
					}
					return;
				}
				this.value = v;
				if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
					return;
				}
			}
		}

		void emit(T v) {

		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				for (; ; ) {
					int s = state;
					if (s == CANCELLED || s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
						return;
					}
					if (s == NO_REQUEST_HAS_VALUE) {
						if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
							Cancellation f = scheduler.schedule(this::emitValue);
							setRequestFuture(f);
						}
						return;
					}
					if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
						return;
					}
				}
			}
		}

		void emitValue() {
			if (fusionState == NO_VALUE) {
				this.fusionState = HAS_VALUE;
			}
			actual.onNext(value);
			if (state != CANCELLED) {
				actual.onComplete();
			}
		}

	}
}

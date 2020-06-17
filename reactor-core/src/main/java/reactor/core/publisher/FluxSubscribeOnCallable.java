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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Executes a Callable and emits its value on the given Scheduler.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSubscribeOnCallable<T> extends Flux<T> implements Fuseable, Scannable {

	final Callable<? extends T> callable;

	final Scheduler scheduler;

	FluxSubscribeOnCallable(Callable<? extends T> callable, Scheduler scheduler) {
		this.callable = Objects.requireNonNull(callable, "callable");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CallableSubscribeOnSubscription<T> parent =
				new CallableSubscribeOnSubscription<>(actual, callable, scheduler);
		actual.onSubscribe(parent);

		try {
			Disposable f = scheduler.schedule(parent);
			parent.setMainFuture(f);
		}
		catch (RejectedExecutionException ree) {
			if(parent.state != CallableSubscribeOnSubscription.HAS_CANCELLED) {
				actual.onError(Operators.onRejectedExecution(ree, actual.currentContext()));
			}
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}

	static final class CallableSubscribeOnSubscription<T>
			implements QueueSubscription<T>, InnerProducer<T>, Runnable {

		final CoreSubscriber<? super T> actual;

		final Callable<? extends T> callable;

		final Scheduler scheduler;

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CallableSubscribeOnSubscription> STATE =
				AtomicIntegerFieldUpdater.newUpdater(CallableSubscribeOnSubscription.class,
						"state");

		T value;
		static final int NO_REQUEST_HAS_VALUE  = 1;
		static final int HAS_REQUEST_NO_VALUE  = 2;
		static final int HAS_REQUEST_HAS_VALUE = 3;
		static final int HAS_CANCELLED         = 4;

		int fusionState;

		static final int NO_VALUE  = 1;
		static final int HAS_VALUE = 2;
		static final int COMPLETE  = 3;

		volatile Disposable mainFuture;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Disposable>
				MAIN_FUTURE = AtomicReferenceFieldUpdater.newUpdater(
				CallableSubscribeOnSubscription.class,
				Disposable.class,
				"mainFuture");

		volatile Disposable requestFuture;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Disposable>
				REQUEST_FUTURE = AtomicReferenceFieldUpdater.newUpdater(
				CallableSubscribeOnSubscription.class,
				Disposable.class,
				"requestFuture");

		CallableSubscribeOnSubscription(CoreSubscriber<? super T> actual,
				Callable<? extends T> callable,
				Scheduler scheduler) {
			this.actual = actual;
			this.callable = callable;
			this.scheduler = scheduler;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return state == HAS_CANCELLED;
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;
			if (key == Attr.RUN_ON) return scheduler;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			state = HAS_CANCELLED;
			fusionState = COMPLETE;
			Disposable a = mainFuture;
			if (a != OperatorDisposables.DISPOSED) {
				a = MAIN_FUTURE.getAndSet(this, OperatorDisposables.DISPOSED);
				if (a != null && a != OperatorDisposables.DISPOSED) {
					a.dispose();
				}
			}
			a = requestFuture;
			if (a != OperatorDisposables.DISPOSED) {
				a = REQUEST_FUTURE.getAndSet(this, OperatorDisposables.DISPOSED);
				if (a != null && a != OperatorDisposables.DISPOSED) {
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
			return fusionState == COMPLETE;
		}

		@Override
		@Nullable
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

		void setMainFuture(Disposable c) {
			for (; ; ) {
				Disposable a = mainFuture;
				if (a == OperatorDisposables.DISPOSED) {
					c.dispose();
					return;
				}
				if (MAIN_FUTURE.compareAndSet(this, a, c)) {
					return;
				}
			}
		}

		void setRequestFuture(Disposable c) {
			for (; ; ) {
				Disposable a = requestFuture;
				if (a == OperatorDisposables.DISPOSED) {
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
				actual.onError(Operators.onOperatorError(this, ex,
						actual.currentContext()));
				return;
			}

			if (v == null) {
				fusionState = COMPLETE;
				actual.onComplete();
				return;
			}

			for (; ; ) {
				int s = state;
				if (s == HAS_CANCELLED || s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
					return;
				}
				if (s == HAS_REQUEST_NO_VALUE) {
					if (fusionState == NO_VALUE) {
						this.value = v;
						this.fusionState = HAS_VALUE;
					}
					actual.onNext(v);
					if (state != HAS_CANCELLED) {
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

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				for (; ; ) {
					int s = state;
					if (s == HAS_CANCELLED || s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
						return;
					}
					if (s == NO_REQUEST_HAS_VALUE) {
						if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
							try {
								Disposable f = scheduler.schedule(this::emitValue);
								setRequestFuture(f);
							}
							catch (RejectedExecutionException ree) {
								actual.onError(Operators.onRejectedExecution(ree,
										actual.currentContext()));
							}
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
			T v = value;
			clear();
			if (v != null) {
				actual.onNext(v);
			}
			if (state != HAS_CANCELLED) {
				actual.onComplete();
			}
		}

	}
}

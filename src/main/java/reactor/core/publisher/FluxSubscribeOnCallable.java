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

	FluxSubscribeOnCallable(Callable<? extends T> callable, Scheduler scheduler) {
		this.callable = Objects.requireNonNull(callable, "callable");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		CallableSubscribeOnSubscription<T> parent =
				new CallableSubscribeOnSubscription<>(s, callable, scheduler);
		s.onSubscribe(parent);

		Cancellation f = scheduler.schedule(parent);
		if (f == Scheduler.REJECTED) {
			if(parent.state != CallableSubscribeOnSubscription.HAS_CANCELLED) {
				s.onError(Operators.onRejectedExecution());
			}
		}
		else {
			parent.setMainFuture(f);
		}
	}

	static final class CallableSubscribeOnSubscription<T>
			implements QueueSubscription<T>, InnerProducer<T>, Runnable {

		final Subscriber<? super T> actual;

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

		CallableSubscribeOnSubscription(Subscriber<? super T> actual,
				Callable<? extends T> callable,
				Scheduler scheduler) {
			this.actual = actual;
			this.callable = callable;
			this.scheduler = scheduler;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case CANCELLED:
					return state == HAS_CANCELLED;
				case BUFFERED:
					return value != null ? 1 : 0;
			}
			return InnerProducer.super.scan(key);
		}

		@Override
		public void cancel() {
			state = HAS_CANCELLED;
			fusionState = COMPLETE;
			Cancellation a = mainFuture;
			if (a != CANCELLED) {
				a = MAIN_FUTURE.getAndSet(this, CANCELLED);
				if (a != null && a != CANCELLED) {
					a.dispose();
				}
			}
			a = requestFuture;
			if (a != CANCELLED) {
				a = REQUEST_FUTURE.getAndSet(this, CANCELLED);
				if (a != null && a != CANCELLED) {
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
				if (a == CANCELLED) {
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
				if (a == CANCELLED) {
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
				if (s == HAS_CANCELLED || s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
					return;
				}
				if(v == null){
					actual.onComplete();
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
							Cancellation f = scheduler.schedule(this::emitValue);
							if(f == Scheduler.REJECTED){
								actual.onError(Operators.onRejectedExecution());
							}
							else {
								setRequestFuture(f);
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

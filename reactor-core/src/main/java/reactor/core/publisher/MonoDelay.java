/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDelay extends Mono<Long> implements Scannable,  SourceProducer<Long>  {

	final Scheduler timedScheduler;

	final long delay;

	final TimeUnit unit;

	MonoDelay(long delay, TimeUnit unit, Scheduler timedScheduler) {
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	@Override
	public void subscribe(CoreSubscriber<? super Long> actual) {
		boolean failOnBackpressure = actual.currentContext().getOrDefault(CONTEXT_OPT_OUT_NOBACKPRESSURE, false) == Boolean.TRUE;

		MonoDelayRunnable r = new MonoDelayRunnable(actual, failOnBackpressure);

		actual.onSubscribe(r);

		try {
			r.setCancel(timedScheduler.schedule(r, delay, unit));
		}
		catch (RejectedExecutionException ree) {
			if(!MonoDelayRunnable.wasCancelled(r.state)) {
				actual.onError(Operators.onRejectedExecution(ree, r, null, null,
						actual.currentContext()));
			}
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timedScheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}

	static final class MonoDelayRunnable implements Runnable, InnerProducer<Long> {
		final CoreSubscriber<? super Long> actual;
		final boolean failOnBackpressure;

		Disposable cancel;

		volatile int state;
		static final AtomicIntegerFieldUpdater<MonoDelayRunnable> STATE = AtomicIntegerFieldUpdater.newUpdater(MonoDelayRunnable.class, "state");

		/** This bit marks the subscription as cancelled */
		static final byte FLAG_CANCELLED       = 0b0100_0_000;
		/** This bit marks the subscription as requested (once) */
		static final byte FLAG_REQUESTED       = 0b0010_0_000;
		/** This bit indicates that a request happened before the delay timer was done (utility, especially in tests) */
		static final byte FLAG_REQUESTED_EARLY = 0b0001_0_000;
		/** This bit indicates that a Disposable was set corresponding to the timer */
		static final byte FLAG_CANCEL_SET      = 0b0000_0_001;
		/** This bit indicates that the timer has expired */
		static final byte FLAG_DELAY_DONE      = 0b0000_0_010;
		/** This bit indicates that, following expiry of the timer AND request, onNext(0L) has been propagated downstream */
		static final byte FLAG_PROPAGATED      = 0b0000_0_100;

		MonoDelayRunnable(CoreSubscriber<? super Long> actual, boolean failOnBackpressure) {
			this.actual = actual;
			this.failOnBackpressure = failOnBackpressure;
		}

		/**
		 * Mark that the cancel {@link Disposable} was set from the {@link java.util.concurrent.Future}.
		 * Immediately returns if it {@link #wasCancelled(int) was cancelled} or if
		 * {@link #wasCancelFutureSet(int) the disposable was already set}.
		 */
		static int markCancelFutureSet(MonoDelayRunnable instance) {
			for (;;) {
				int state = instance.state;
				if (wasCancelled(state) || wasCancelFutureSet(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | FLAG_CANCEL_SET)) {
					return state;
				}
			}
		}

		/**
		 * @return true if the {@link #FLAG_CANCEL_SET} bit is set on the given state
		 */
		static boolean wasCancelFutureSet(int state) {
			return (state & FLAG_CANCEL_SET) == FLAG_CANCEL_SET;
		}

		/**
		 * Mark the subscriber has been cancelled. Immediately returns if it
		 * {@link #wasCancelled(int) was already cancelled}.
		 */
		static int markCancelled(MonoDelayRunnable instance) {
			for (;;) {
				int state = instance.state;
				if (wasCancelled(state) || wasPropagated(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | FLAG_CANCELLED)) {
					return state;
				}
			}
		}

		/**
		 * @return true if the {@link #FLAG_CANCELLED} bit is set on the given state
		 */
		static boolean wasCancelled(int state) {
			return (state & FLAG_CANCELLED) == FLAG_CANCELLED;
		}

		/**
		 * Mark the delay has run out. Immediately returns if it
		 * {@link #wasCancelled(int) was already cancelled} or already
		 * {@link #wasDelayDone(int) marked as done}.
		 */
		static int markDelayDone(MonoDelayRunnable instance) {
			for (;;) {
				int state = instance.state;
				if (wasCancelled(state) || wasDelayDone(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | FLAG_DELAY_DONE)) {
					return state;
				}
			}
		}

		/**
		 * @return true if the {@link #FLAG_DELAY_DONE} bit is set on the given state
		 */
		static boolean wasDelayDone(int state) {
			return (state & FLAG_DELAY_DONE) == FLAG_DELAY_DONE;
		}

		/**
		 * Mark the operator has been requested. Immediately returns if it
		 * {@link #wasCancelled(int) was already cancelled} or already
		 * {@link #wasRequested(int) requested}. Also sets the {@link #FLAG_REQUESTED_EARLY}
		 * flag if the delay was already done when request happened.
		 */
		static int markRequested(MonoDelayRunnable instance) {
			for (;;) {
				int state = instance.state;
				if (wasCancelled(state) || wasRequested(state)) {
					return state;
				}
				int newFlag = FLAG_REQUESTED;
				if (!wasDelayDone(state)) {
					newFlag = newFlag | FLAG_REQUESTED_EARLY;
				}

				if (STATE.compareAndSet(instance, state, state | newFlag)) {
					return state;
				}
			}
		}

		/**
		 * @return true if the {@link #FLAG_REQUESTED} bit is set on the given state
		 */
		static boolean wasRequested(int state) {
			return (state & FLAG_REQUESTED) == FLAG_REQUESTED;
		}

		/**
		 * Mark the operator has emitted the tick. Immediately returns if it
		 * {@link #wasCancelled(int) was already cancelled}.
		 */
		static int markPropagated(MonoDelayRunnable instance) {
			for (;;) {
				int state = instance.state;
				if (wasCancelled(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | FLAG_PROPAGATED)) {
					return state;
				}
			}
		}

		/**
		 * @return true if the {@link #FLAG_PROPAGATED} bit is set on the given state
		 */
		static boolean wasPropagated(int state) {
			return (state & FLAG_PROPAGATED) == FLAG_PROPAGATED;
		}


		@Override
		public CoreSubscriber<? super Long> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return wasDelayDone(this.state) && wasRequested(this.state);
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return wasRequested(this.state) ? 1L : 0L;
			if (key == Attr.CANCELLED) return wasCancelled(this.state);
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		void setCancel(Disposable cancel) {
			Disposable c = this.cancel;
			this.cancel = cancel;
			int previousState = markCancelFutureSet(this);
			if (wasCancelFutureSet(previousState)) {
				if (c != null) {
					c.dispose();
				}
				return;
			}
			if (wasCancelled(previousState)) {
				cancel.dispose();
			}
		}

		private void propagateDelay() {
			int previousState = markPropagated(this);
			if (wasCancelled(previousState)) {
				return;
			}
			try {
				actual.onNext(0L);
				actual.onComplete();
			}
			catch (Throwable t){
				actual.onError(Operators.onOperatorError(t, actual.currentContext()));
			}
		}

		@Override
		public void run() {
			int previousState = markDelayDone(this);
			if (wasCancelled(previousState) || wasDelayDone(previousState)) {
				return;
			}
			if (wasRequested(previousState)) {
				propagateDelay();
			}
			else if (failOnBackpressure) {
				actual.onError(Exceptions.failWithOverflow("Could not emit value due to lack of requests"));
			}
		}

		@Override
		public void cancel() {
			int previousState = markCancelled(this);
			if (wasCancelled(previousState) || wasPropagated(previousState)) {
				//ignore
				return;
			}
			if (wasCancelFutureSet(previousState)) {
				this.cancel.dispose();
			}
			//otherwise having marked cancelled will trigger immediate disposal upon setCancel
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				int previousState = markRequested(this);
				if (wasCancelled(previousState) || wasRequested(previousState)) {
					return;
				}
				if (wasDelayDone(previousState) && !failOnBackpressure) {
					propagateDelay();
				}
			}
		}
	}

	static final String CONTEXT_OPT_OUT_NOBACKPRESSURE = "reactor.core.publisher.MonoDelay.failOnBackpressure";
}

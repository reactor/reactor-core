/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static reactor.core.Fuseable.ASYNC;

/**
 * @author David Karnok
 */
final class FluxWindowTimeout<T> extends InternalFluxOperator<T, Flux<T>> {

	final int       maxSize;
	final long      timespan;
	final TimeUnit  unit;
	final Scheduler timer;
	final boolean   fairBackpressure;

	FluxWindowTimeout(Flux<T> source,
			int maxSize,
			long timespan,
			TimeUnit unit,
			Scheduler timer,
			boolean fairBackpressure) {
		super(source);
		if (timespan <= 0) {
			throw new IllegalArgumentException("Timeout period must be strictly positive");
		}
		if (maxSize <= 0) {
			throw new IllegalArgumentException("maxSize must be strictly positive");
		}
		this.fairBackpressure = fairBackpressure;
		this.timer = Objects.requireNonNull(timer, "Timer");
		this.timespan = timespan;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.maxSize = maxSize;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Flux<T>> actual) {
		if (fairBackpressure) {
			return new WindowTimeoutWithBackpressureSubscriber<>(actual, maxSize, timespan, unit, timer);
		}
		return new WindowTimeoutSubscriber<>(actual, maxSize, timespan, unit, timer);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) {
			return timer;
		}
		if (key == Attr.RUN_STYLE) {
			return Attr.RunStyle.ASYNC;
		}

		return super.scanUnsafe(key);
	}

	static final class WindowTimeoutWithBackpressureSubscriber<T>
			implements InnerOperator<T, Flux<T>> {

		final CoreSubscriber<? super Flux<T>> actual;
		final long                            timespan;
		final TimeUnit                        unit;
		final Scheduler                       scheduler;
		final int                             maxSize;
		final Scheduler.Worker                worker;
		final int                             limit;
		final Queue<String> signals = new ArrayBlockingQueue<>(500);

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutWithBackpressureSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, "requested");


		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutWithBackpressureSubscriber> STATE =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, "state");

		static final long CANCELLED_FLAG       =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TERMINATED_FLAG       =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_UNSENT_WINDOW    =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_WORK_IN_PROGRESS =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long STAMP_INDEX_MASK     =
				0b0000_1111_1111_1111_1111_1111_1111_1111_1000_0000_0000_0000_0000_0000_0000_0000L;
		static final long WINDOW_INDEX_MASK    =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0111_1111_1111_1111_1111_1111_1111_1111L;
		static final int  STAMP_INDEX_SHIFT    = 31;


		long producerIndex;
		boolean done;
		Throwable error;

		Subscription s;

		volatile InnerWindow<T> window;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowTimeoutWithBackpressureSubscriber, InnerWindow> WINDOW =
				AtomicReferenceFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, InnerWindow.class, "window");

		WindowTimeoutWithBackpressureSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int maxSize,
				long timespan,
				TimeUnit unit,
				Scheduler scheduler) {
			this.actual = actual;
			this.timespan = timespan;
			this.unit = unit;
			this.scheduler = scheduler;
			this.maxSize = maxSize;
			this.limit = Operators.unboundedOrLimit(maxSize);
			this.worker = scheduler.createWorker();
		}

		@Override
		public CoreSubscriber<? super Flux<T>> actual() {
			return this.actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				this.actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			final long producerIndex = this.producerIndex;
			this.producerIndex = producerIndex + 1;

			while (true) {
				final InnerWindow<T> window = this.window;
				if (InnerWindow.TERMINATED == window) {
					Operators.onDiscard(t, this.currentContext());
					return;
				}

				if (window.sendNext(t)) {
					return;
				}
			}
		}

		@Override
		public void onError(Throwable t) {

		}

		@Override
		public void onComplete() {

		}

		static boolean hasUnsentWindow(long state) {
			return (state & HAS_UNSENT_WINDOW) == HAS_UNSENT_WINDOW;
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}

		static long incrementStamp(long state) {
			return (((state & STAMP_INDEX_MASK) >> STAMP_INDEX_SHIFT + 1) << STAMP_INDEX_SHIFT) & STAMP_INDEX_MASK;
		}

		static boolean hasWorkInProgress(long state) {
			return (state & HAS_WORK_IN_PROGRESS) == HAS_WORK_IN_PROGRESS;
		}

		static long incrementWindowIndex(long state) {
			return ((state & WINDOW_INDEX_MASK) + 1) & WINDOW_INDEX_MASK;
		}

		static int windowIndex(long state) {
			return (int) (state & WINDOW_INDEX_MASK);
		}

		static <T> long markWorkDone(WindowTimeoutWithBackpressureSubscriber<T> instance, long expectedState, boolean incrementIndex, boolean setUnsetFlag) {
			for (;;) {
				final long currentState = instance.state;

				if (expectedState != currentState) {
					return currentState;
				}

				final long nextState = (incrementIndex
						? (currentState &~ WINDOW_INDEX_MASK) ^ HAS_WORK_IN_PROGRESS | incrementWindowIndex(currentState)
						: currentState ^ HAS_WORK_IN_PROGRESS) |
						(setUnsetFlag ? HAS_UNSENT_WINDOW : 0);
				if (STATE.compareAndSet(instance, currentState, nextState)) {
//					this.signals.add("Added_request_" + Thread.currentThread() + "_" + state + "-" + (HAS_ACTIVE_WINDOW | nextRequested));
					return nextState;
				}
			}
		}

		@Override
		public void request(long n) {
			final long previousRequested = Operators.addCap(REQUESTED, this, n);
			if (previousRequested == Long.MAX_VALUE) {
				return;
			}

			long previousState;
			long expectedState;
			boolean hasUnsentWindow;
			for (;;) {
			    previousState = this.state;

				if (isCancelled(previousState)) {
					return;
				}

				hasUnsentWindow = hasUnsentWindow(previousState);

				if (isTerminated(previousState) && !hasUnsentWindow) {
					return;
				}

				if ((windowIndex(previousState) != 0 && !hasUnsentWindow) || hasWorkInProgress(previousState)) {
					if (STATE.compareAndSet(this, previousState, previousState &~ STAMP_INDEX_MASK | incrementStamp(previousState))) {
						return;
					}

					continue;
				}

				expectedState =
						previousState ^ (hasUnsentWindow ? HAS_UNSENT_WINDOW : 0) |
						HAS_WORK_IN_PROGRESS;
				if (STATE.compareAndSet(this, previousState, expectedState)) {
//					this.signals.add("Added_request_" + Thread.currentThread() + "_" + state + "-" + (HAS_ACTIVE_WINDOW | nextRequested));
					break;
				}
			}

			drain(previousState, expectedState);
		}

		void drain(long previousState, long expectedState) {
			for (;;) {
				long n = this.requested;

				if (n > 0) {
					boolean hasUnsentWindow = hasUnsentWindow(previousState);
					if (hasUnsentWindow) {
						final InnerWindow<T> currentUnsentWindow = this.window;

						this.actual.onNext(currentUnsentWindow);

						if (n != Long.MAX_VALUE) {
							n = REQUESTED.decrementAndGet(this);
						}

						if (isTerminated(expectedState)) {
							Throwable e = this.error;
							if (e != null) {
								this.actual.onError(e);
							}
							else {
								this.actual.onComplete();
							}
							return;
						}

						final long previousInnerWindowState = InnerWindow.markSent(currentUnsentWindow);
						if (InnerWindow.isTimeout(previousInnerWindowState)a || InnerWindow.isTerminated(previousInnerWindowState)) {
							final boolean shouldBeUnsent = n <= 0;
							final InnerWindow<T> nextWindow = new InnerWindow<>(this.maxSize, this, shouldBeUnsent);

							this.window = nextWindow;

							if (!shouldBeUnsent) {
								this.actual.onNext(nextWindow);

								if (n != Long.MAX_VALUE) {
									REQUESTED.decrementAndGet(this);
								}
							}

							previousState = expectedState;
							expectedState = markWorkDone(this,
									expectedState,
									true,
									shouldBeUnsent);

							if (isCancelled(expectedState)) {
								if (shouldBeUnsent) {
									Operators.onDiscard(nextWindow, this.actual.currentContext());
								}
								return;
							}

							if (isTerminated(expectedState)) {
								final Throwable e = this.error;
								if (e != null) {
									this.actual.onError(e);
								}
								else {
									this.actual.onComplete();
								}
								return;
							}

							if (!hasWorkInProgress(expectedState)) {
								nextWindow.scheduleTimeout(); // TODO: try-catch

								final long received = InnerWindow.received(previousInnerWindowState);
								if (received > 0) {
									this.s.request(received);
								}

								return;
							}

							// TODO: ???
						}
						else {
							final InnerWindow<T> nextWindow = new InnerWindow<>(this.maxSize, this, false);

							this.window = nextWindow;

							this.actual.onNext(nextWindow);

							if (n != Long.MAX_VALUE) {
								REQUESTED.decrementAndGet(this);
							}

							expectedState = markWorkDone(this, expectedState, true, false);

							if (isCancelled(expectedState)) {
								return;
							}

							if (isTerminated(expectedState)) {
								final Throwable e = this.error;
								if (e != null) {
									this.actual.onError(e);
								}
								else {
									this.actual.onComplete();
								}
								return;
							}

							if (!hasWorkInProgress(expectedState)) {
								nextWindow.scheduleTimeout(); // TODO: try-catch

								this.s.request(this.maxSize);
							}
						}
					}
				} else {
					expectedState = markWorkDone(this, expectedState, false, false);

					if (isCancelled(expectedState)) {
						return;
					}

					if (isTerminated(expectedState) && !hasUnsentWindow(previousState)) {
						final Throwable e = this.error;
						if (e != null) {
							this.actual.onError(e);
						}
						else {
							this.actual.onComplete();
						}
						return;
					}

					if (!hasWorkInProgress(expectedState)) {
						return;
					}

					// TODO: ???
				}
			}
		}

		@Override
		public void cancel() {
			this.s.cancel();
			for (; ; ) {
				final InnerWindow<T> window = this.window;
				if (window == InnerWindow.TERMINATED) {
					return;
				}

				if (window != null) {
					final long previousState = window.cancelByParent();
					if (!InnerWindow.isSent(previousState) && !InnerWindow.isSending(previousState)) {
						Operators.onDiscard(window, this.currentContext());
						return;
					}
				}

				if (WINDOW.compareAndSet(this, window, InnerWindow.TERMINATED)) {
					this.signals.add("cancelled");
					return;
				}
			}
		}

		void tryCreateNextWindow(int windowIndex) {
			long previousState;
			long expectedState;
			boolean hasUnsentWindow;
			for (;;) {
				previousState = this.state;

				if (isCancelled(previousState)) {
					return;
				}

				hasUnsentWindow = hasUnsentWindow(previousState);

				if (isTerminated(previousState) && !hasUnsentWindow) {
					return;
				}

				if (windowIndex(previousState) != windowIndex) {
					return;
				}

				if (hasWorkInProgress(previousState)) {
					if (STATE.compareAndSet(this, previousState, previousState &~ STAMP_INDEX_MASK | incrementStamp(previousState))) {
						return;
					}

					continue;
				}

				expectedState = previousState | HAS_WORK_IN_PROGRESS;
				if (STATE.compareAndSet(this, previousState, expectedState)) {
//					this.signals.add("Added_request_" + Thread.currentThread() + "_" + state + "-" + (HAS_ACTIVE_WINDOW | nextRequested));
					break;
				}
			}

			drain(previousState, expectedState);
		}

		Disposable schedule(Runnable runnable, long createTime) {
			final long delayedNanos = scheduler.now(TimeUnit.NANOSECONDS) - createTime;

			final long timeSpanInNanos = unit.toNanos(timespan);
			final long newTimeSpanInNanos = timeSpanInNanos - delayedNanos;
			if (newTimeSpanInNanos > 0) {
				return worker.schedule(runnable, timespan, unit);
			} else {
				runnable.run();
				return InnerWindow.DISPOSED;
			}
		}

		long now() {
			return scheduler.now(TimeUnit.NANOSECONDS);
		}
	}

	static final class InnerWindow<T> extends Flux<T>
			implements InnerProducer<T>, Fuseable.QueueSubscription<T>, Runnable {

		static final InnerWindow<?> TERMINATED = new InnerWindow<>();
		static final Disposable DISPOSED = Disposables.disposed();

		final WindowTimeoutWithBackpressureSubscriber<T> parent;
		final int                                        max;
		final Queue<T>                                   queue;
		final long                                       createTime;
		final long                                       random = ThreadLocalRandom.current().nextInt();

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerWindow> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerWindow.class, "requested");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerWindow> STATE =
				AtomicLongFieldUpdater.newUpdater(InnerWindow.class, "state");

		volatile Disposable timer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<InnerWindow, Disposable> TIMER =
				AtomicReferenceFieldUpdater.newUpdater(InnerWindow.class, Disposable.class, "timer");


		CoreSubscriber<? super T> actual;

		Throwable error;

		int mode;

		int received = 0;
		int produced = 0;

		 InnerWindow(
				int max,
				WindowTimeoutWithBackpressureSubscriber<T> parent,
				boolean markUnsent) {
			this.max = max;
			this.parent = parent;
			this.queue = Queues.<T>get(max).get();

			if (markUnsent) {
				STATE.lazySet(this, UNSENT_STATE);
			}

			parent.signals.offer(Arrays.toString(new RuntimeException().getStackTrace()) + this +
					"-" + Thread.currentThread().getId() +
				"-wct-" + formatState(state)+"-" + formatState(state));

			this.createTime = parent.now();
		}

		private InnerWindow() {
			this.max = Integer.MAX_VALUE;
			this.parent = null;
			this.queue = null;
			this.createTime = Long.MAX_VALUE;
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> actual) {
			long previousState = markSubscribedOnce(this);
			if (hasSubscribedOnce(previousState)) {
				Operators.error(actual,
						new IllegalStateException("Only one subscriber allowed"));
				return;
			}

			this.actual = actual;

			actual.onSubscribe(this);

			previousState = markSubscriberSet(this);
			if (isFinalized(previousState) || hasWorkInProgress(previousState)) {
				return;
			}

			if (!hasValues(previousState) && isTerminated(previousState)) {
				if (mode == ASYNC) {
					this.actual.onComplete();
					return;
				}

				clearAndFinalize();
				actual.onComplete();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public void request(long n) {
			if (this.mode == ASYNC) {
				//noinspection ConstantConditions
				this.actual.onNext(null);
				return;
			}

			Operators.addCap(REQUESTED, this, n);
			final long previousState = markHasRequest(this);

			if (hasWorkInProgress(previousState) || isCancelled(previousState) || isFinalized(previousState)) {
				return;
			}

			if (hasValues(previousState)) {
				drain((previousState | HAS_SUBSCRIBER_SET_STATE) + 1);
			}
		}

		@Override
		public void cancel() {
			final long previousState = markCancelled(this);
			if (isCancelled(previousState) || isFinalized(previousState) || hasWorkInProgress(previousState)) {
				return;
			}

			clearAndFinalize();
		}

		long cancelByParent() {
			for (;;) {
				final long state = this.state;

				if (isCancelledByParent(state)) {
					return state;
				}

				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				final long nextState = cleanState |
						TERMINATED_STATE |
						CANCELLED_PARENT_STATE |
						(
							hasSubscriberSet(state)
								? hasValues(state)
									? incrementWork(state & WORK_IN_PROGRESS_MAX)
									: FINALIZED_STATE
								: 0
						);

				if (STATE.compareAndSet(this, state, nextState)) {
					this.parent.signals.offer(this + "-" + Thread.currentThread().getId() + "-cfp-" + formatState(state)+"-" + formatState(nextState));

					if (isFinalized(state)) {
						return state;
					}

					if (!isTimeout(state)) {
						final Disposable timer = TIMER.getAndSet(this, DISPOSED);
						if (timer != null) {
							timer.dispose();
						}
					}

					if (hasSubscriberSet(state)) {
						if (this.mode == ASYNC) {
							this.actual.onComplete();
							return state;
						}

						if (hasWorkInProgress(state)) {
							return state;
						}

						if (isCancelled(state)) {
							clearAndFinalize();
							return state;
						}

						if (hasValues(state)) {
							drain(nextState);
						} else {
							this.actual.onComplete();
						}
					}

					return state;
				}
			}
		}

		boolean sendNext(T t) {
			int received = this.received + 1 ;
			this.received = received;

			this.queue.offer(t);

			final long previousState;
			final long nextState;
			if (received == this.max) {
				previousState = markHasValuesAndTerminated(this);
				nextState = (previousState | TERMINATED_STATE | HAS_VALUES_STATE) + 1;

				if (!isTimeout(previousState)) {
					final Disposable timer = TIMER.getAndSet(this, DISPOSED);
					if (timer != null) {
						timer.dispose();
					}

					this.parent.tryCreateNextWindow();
				}
			} else {
				previousState = markHasValues(this);
				nextState = (previousState | HAS_VALUES_STATE) + 1;
			}

			if (isFinalized(previousState)) {
				if (!isCancelledBySubscriberOrByParent(previousState)) {
					queue.poll();
					return false;
				} else {
					clearQueue();
					return true;
				}
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onNext(t);
					return true;
				}

				if (hasWorkInProgress(previousState)) {
					return true;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return true;
				}

				drain(nextState);
			}

			return true;
		}

		long sendCompleteByParent() {
			final long previousState = markTerminatedByParent(this);
			if (isFinalized(previousState) || isTerminated(previousState)) {
				return previousState;
			}

			if (!isTimeout(previousState)) {
				final Disposable timer = TIMER.getAndSet(this, DISPOSED);

				if (timer != null) {
					timer.dispose();
				}
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onComplete();
					return previousState;
				}

				if (hasWorkInProgress(previousState)) {
					return previousState;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return previousState;
				}

				if (hasValues(previousState)) {
					drain((previousState | TERMINATED_STATE | PARENT_TERMINATED_STATE) + 1);
				} else {
					this.actual.onComplete();
				}
			}

			return previousState;
		}

		long sendErrorByParent(Throwable error) {
			 this.error = error;

			final long previousState = markTerminatedByParent(this);
			if (isFinalized(previousState) || isTerminated(previousState)) {
				return previousState;
			}

			if (!isTimeout(previousState)) {
				final Disposable timer = TIMER.getAndSet(this, DISPOSED);

				if (timer != null) {
					timer.dispose();
				}
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onError(error);
					return previousState;
				}

				if (hasWorkInProgress(previousState)) {
					return previousState;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return previousState;
				}

				if (hasValues(previousState)) {
					drain((previousState | TERMINATED_STATE | PARENT_TERMINATED_STATE) + 1);
				} else {
					this.actual.onError(error);
				}
			}

			return previousState;
		}

		void drain(long expectedState) {
			final Queue<T> q = this.queue;
			final CoreSubscriber<? super T> a = this.actual;

			for (; ; ) {
				long r = this.requested;
				int e = 0;
				boolean empty = false;
				while (e < r) {
					final T v = q.poll();

					empty = v == null;
					if (checkTerminated(this.produced + e, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
				}

				this.produced += e;

				if (checkTerminated(this.produced, a, null)) {
					return;
				}

				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				expectedState = markWorkDone(this, expectedState, !empty);
				if (isCancelled(expectedState)) {
					clearAndFinalize();
					return;
				}

				if (!hasWorkInProgress(expectedState)) {
					return;
				}
			}
		}

		boolean checkTerminated(
				int totalProduced,
				CoreSubscriber<? super T> actual,
				@Nullable T value) {
			final long state = this.state;
			if (isCancelled(state)) {
				if (value != null) {
					Operators.onDiscard(value, actual.currentContext());
				}
				clearAndFinalize();
				return true;
			}

			if (received(state) == totalProduced && isTerminated(state)) {
				clearAndFinalize();

				final Throwable e = this.error;
				if (e != null) {
					actual.onError(e);
				}
				else {
					actual.onComplete();
				}

				return true;
			}

			return false;
		}

		void scheduleTimeout() {
			final Disposable nextTimer = parent.schedule(this, this.createTime);
			if (!TIMER.compareAndSet(this, null, nextTimer)) {
				nextTimer.dispose();
			}
		}

		@Override
		public void run() {
			long previousState = markTimeout(this);
			if (isTerminated(previousState)) {
				return;
			}

			if (isCancelledByParent(previousState)) {
				return;
			}

			this.parent.tryCreateNextWindow();
		}

		@Override
		public T poll() {
			return queue.poll();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) == ASYNC) {
				this.mode = ASYNC;
				return ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		public int size() {
			return this.queue.size();
		}

		@Override
		public boolean isEmpty() {
			return this.queue.isEmpty();
		}

		@Override
		public void clear() {
			clearAndFinalize();
		}

		void clearAndFinalize() {
			for (; ; ) {
				long state = this.state;

				clearQueue();

				if (isFinalized(state)) {
					return;
				}

				final long nextState = ((state | FINALIZED_STATE) & ~WORK_IN_PROGRESS_MAX) ^ (hasValues(state) ? HAS_VALUES_STATE : 0);
				if (STATE.compareAndSet(this, state, nextState)) {
					this.parent.signals.offer(Arrays.toString(new RuntimeException().getStackTrace()));
					this.parent.signals.offer(this + "-" + Thread.currentThread().getId() + "-mfi-" + formatState(state)+"-" + formatState(nextState));
					return;
				}
			}
		}

		void clearQueue() {
			final Queue<T> q = this.queue;
			final Context context = this.actual.currentContext();

			T v;
			while ((v = q.poll()) != null) {
				Operators.onDiscard(v, context);
			}
		}

		static final long FINALIZED_STATE          =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TERMINATED_STATE         =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long PARENT_TERMINATED_STATE  =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long CANCELLED_STATE          =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TIMEOUT_STATE            =
				0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long CANCELLED_PARENT_STATE   =
				0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_VALUES_STATE         =
				0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_STATE     =
				0b0000_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_SET_STATE =
				0b0000_0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long UNSENT_STATE             =
				0b0000_0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long WORK_IN_PROGRESS_MAX     =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0011_1111_1111_1111_1111_1111L;
		static final long RECEIVED_MASK            =
				0b0000_0000_0001_1111_1111_1111_1111_1111_1111_1111_1100_0000_0000_0000_0000_0000L;
		static final long RECEIVED_SHIFT_BITS      = 22;

		long s =
				0b0000_0010_0100_0000_0000_0000_0000_0000_0000_0000_0010_0000_0000_0000_0000_0000L;

		static <T> long markSent(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long nextState =
						(state ^ UNSENT_STATE) |
						(isTimeout(state) ? TERMINATED_STATE : 0) |
								(
									hasSubscriberSet(state)
											? hasValues(state)
												? incrementWork(state & WORK_IN_PROGRESS_MAX)
                                                : FINALIZED_STATE
											: 0
								);
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mst-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean isSent(long state) {
			return (state & UNSENT_STATE) == 0;
		}

		static <T> long markSending(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isSent(state) || isFinalized(state)) {
					return state;
				}

				final long nextState = state | UNSENT_STATE;
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-msg-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static <T> long markTimeout(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isTerminated(state)) {
					return state;
				}

				final long nextState = state | TIMEOUT_STATE;
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mti-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean isTimeout(long state) {
			return (state & TIMEOUT_STATE) == TIMEOUT_STATE;
		}

		static <T> long markCancelled(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state) || isFinalized(state)) {
					return state;
				}

				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				final long nextState =
						cleanState | CANCELLED_STATE | incrementWork(state & WORK_IN_PROGRESS_MAX);
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(Arrays.toString(new RuntimeException().getStackTrace()) + instance +
							"-" + Thread.currentThread().getId() + "-mcd-" + formatState(state)+"-" + formatState(
							nextState));
					return state;
				}
			}
		}

		static boolean isCancelledBySubscriberOrByParent(long state) {
			return (state & CANCELLED_STATE) == CANCELLED_STATE  || (state & CANCELLED_PARENT_STATE) == CANCELLED_PARENT_STATE;
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_STATE) == CANCELLED_STATE;
		}

		static boolean isCancelledByParent(long state) {
			return (state & CANCELLED_PARENT_STATE) == CANCELLED_PARENT_STATE;
		}

		static <T> long markHasValues(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isFinalized(state)) {
					return state;
				}

				final long nextState;
				if (hasSubscriberSet(state)) {
					if (instance.mode == ASYNC) {
						return state;
					}
					final long cleanState =
							(state & ~WORK_IN_PROGRESS_MAX) & ~RECEIVED_MASK;
					nextState = cleanState |
							HAS_VALUES_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							incrementWork(state & WORK_IN_PROGRESS_MAX);
				}
				else {
					final long cleanState =
							(state & ~WORK_IN_PROGRESS_MAX) & ~RECEIVED_MASK;
					nextState = cleanState |
							HAS_VALUES_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							(hasWorkInProgress(state) ? incrementWork(state & WORK_IN_PROGRESS_MAX) : 0);
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mhv-" + formatState(state) + "-" + formatState(nextState));
					return state;
				}
			}
		}

		static <T> long markHasValuesAndTerminated(InnerWindow<T> instance) {
			for (;;) {
				final long state = instance.state;

				if (isFinalized(state)) {
					return state;
				}

				final long nextState;
				if (hasSubscriberSet(state)) {
					if (instance.mode == ASYNC) {
						return state;
					}

					final long cleanState =
							(state & ~WORK_IN_PROGRESS_MAX) & ~RECEIVED_MASK;
					nextState = cleanState |
							HAS_VALUES_STATE |
							TERMINATED_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							incrementWork(state & WORK_IN_PROGRESS_MAX);
				}
				else {
					final long cleanState = state & ~WORK_IN_PROGRESS_MAX & ~RECEIVED_MASK;
					nextState = cleanState |
							HAS_VALUES_STATE |
							TERMINATED_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							(hasWorkInProgress(state) ? incrementWork(state & WORK_IN_PROGRESS_MAX) : 0);
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mht-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean hasValues(long state) {
			return (state & HAS_VALUES_STATE) == HAS_VALUES_STATE;
		}

		static long received(long state) {
			return ((state & RECEIVED_MASK) >> RECEIVED_SHIFT_BITS);
		}

		static <T> long markHasRequest(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state) || isFinalized(state)) {
					return state;
				}

				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				final long nextState = cleanState |
						HAS_SUBSCRIBER_SET_STATE |
						(
							hasValues(state)
								? incrementWork(state & WORK_IN_PROGRESS_MAX)
								: 0
						);

				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mhr-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static <T> long markTimeoutProcessedAndTerminated(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				final long nextState =
						cleanState |
								TERMINATED_STATE |
								(
										hasSubscriberSet(state)
												? hasValues(state)
													? incrementWork(state & WORK_IN_PROGRESS_MAX)
													: FINALIZED_STATE
												: 0
								);
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mte-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static <T> long markTerminatedByParent(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelledByParent(state) || isTerminatedByParent(state)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mtp-" + formatState(state)+"-" + formatState(state));
					return state;
				}


				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				long nextState = cleanState |
								TERMINATED_STATE |
								PARENT_TERMINATED_STATE;

				if (hasSubscriberSet(state)) {
					if (hasValues(state)) {
						nextState |= incrementWork(state & WORK_IN_PROGRESS_MAX);
					} else {
						nextState |= FINALIZED_STATE;
					}
				} else if (!hasSubscribedOnce(state) && !hasValues(state)) {
					nextState |= FINALIZED_STATE;
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mtp-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_STATE) == TERMINATED_STATE;
		}

		static boolean isTerminatedByParent(long state) {
			return (state & PARENT_TERMINATED_STATE) == PARENT_TERMINATED_STATE;
		}

		static long incrementWork(long currentWork) {
			return currentWork == WORK_IN_PROGRESS_MAX
					? 1
					: (currentWork + 1);
		}

		static long incrementReceived(long currentWork) {
			return ((currentWork >> RECEIVED_SHIFT_BITS) + 1) << RECEIVED_SHIFT_BITS;
		}

		static <T> long markSubscribedOnce(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (hasSubscribedOnce(state)) {
					return state;
				}

				final long nextState = state | HAS_SUBSCRIBER_STATE;
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mso-" + formatState(state)+"-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean hasSubscribedOnce(long state) {
			return (state & HAS_SUBSCRIBER_STATE) == HAS_SUBSCRIBER_STATE;
		}

		static <T> long markSubscriberSet(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isFinalized(state) || hasWorkInProgress(state)) {
					return state;
				}

				final long nextState =
						(state | HAS_SUBSCRIBER_SET_STATE) + (isTerminated(state) && !hasValues(state) ? 1 : 0);
				if (STATE.compareAndSet(instance, state, nextState)) {
					instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mss-" + formatState(state) + "-" + formatState(nextState));
					return state;
				}
			}
		}

		static boolean hasSubscriberSet(long state) {
			return (state & HAS_SUBSCRIBER_SET_STATE) == HAS_SUBSCRIBER_SET_STATE;
		}

		static <T> long markWorkDone(InnerWindow<T> instance,
				long expectedState,
				boolean hasValues) {
			final long state = instance.state;
			if (expectedState != state) {
				return state;
			}

			final long nextState =
					(state ^ (hasValues ? 0 : HAS_VALUES_STATE)) &~ WORK_IN_PROGRESS_MAX;
			if (STATE.compareAndSet(instance, state, nextState)) {
				instance.parent.signals.offer(instance + "-" + Thread.currentThread().getId() + "-mwd-" + formatState(state)+"-" + formatState(nextState));
				return nextState;
			}

			return instance.state;
		}

		static String formatState(long state) {
			final String defaultFormat = Long.toBinaryString(state);
			final StringBuilder formatted =
					new StringBuilder();
			final int toPrepend = 64 - defaultFormat.length();
			for (int i = 0; i < 64; i++) {
				if (i != 0 && i % 4 == 0) {
					formatted.append("_");
				}
				if (i < toPrepend) {
					formatted.append("0");
				} else {
					formatted.append(defaultFormat.charAt(i - toPrepend));
				}
			}

			formatted.insert(0, "0b");
			return formatted.toString();
		}

		static boolean hasWorkInProgress(long state) {
			return (state & WORK_IN_PROGRESS_MAX) > 0;
		}

		static boolean isFinalized(long state) {
			return (state & FINALIZED_STATE) == FINALIZED_STATE;
		}

		@Override
		public String toString() {
			return super.toString() + " " + random;
		}
	}

	static final class WindowTimeoutSubscriber<T> implements InnerOperator<T, Flux<T>> {

		final CoreSubscriber<? super Flux<T>> actual;
		final long                            timespan;
		final TimeUnit                        unit;
		final Scheduler                       scheduler;
		final int                             maxSize;
		final Scheduler.Worker                worker;
		final Queue<Object>                   queue;

		Throwable error;
		volatile boolean done;
		volatile boolean cancelled;

		volatile     long                                            requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutSubscriber.class,
						"requested");

		volatile     int                                                wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowTimeoutSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class,
						"wip");

		int  count;
		long producerIndex;

		Subscription s;

		Sinks.Many<T> window;

		volatile boolean terminated;

		volatile     Disposable timer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowTimeoutSubscriber, Disposable>
		                        TIMER = AtomicReferenceFieldUpdater.newUpdater(
				WindowTimeoutSubscriber.class,
				Disposable.class,
				"timer");

		WindowTimeoutSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int maxSize,
				long timespan,
				TimeUnit unit,
				Scheduler scheduler) {
			this.actual = actual;
			this.queue = Queues.unboundedMultiproducer()
			                   .get();
			this.timespan = timespan;
			this.unit = unit;
			this.scheduler = scheduler;
			this.maxSize = maxSize;
			this.worker = scheduler.createWorker();
		}

		@Override
		public CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			Sinks.Many<T> w = window;
			return w == null ? Stream.empty() : Stream.of(Scannable.from(w));
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.CANCELLED) {
				return cancelled;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}
			if (key == Attr.CAPACITY) {
				return maxSize;
			}
			if (key == Attr.BUFFERED) {
				return queue.size();
			}
			if (key == Attr.RUN_ON) {
				return worker;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.ASYNC;
			}
			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				Subscriber<? super Flux<T>> a = actual;

				a.onSubscribe(this);

				if (cancelled) {
					return;
				}

				Sinks.Many<T> w = Sinks.unsafe()
				                       .many()
				                       .unicast()
				                       .onBackpressureBuffer();
				window = w;

				long r = requested;
				if (r != 0L) {
					a.onNext(w.asFlux());
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
				}
				else {
					a.onError(Operators.onOperatorError(s,
							Exceptions.failWithOverflow(),
							actual.currentContext()));
					return;
				}

				if (OperatorDisposables.replace(TIMER, this, newPeriod())) {
					s.request(Long.MAX_VALUE);
				}
			}
		}

		Disposable newPeriod() {
			try {
				return worker.schedulePeriodically(new ConsumerIndexHolder(producerIndex,
						this), timespan, timespan, unit);
			}
			catch (Exception e) {
				actual.onError(Operators.onRejectedExecution(e,
						s,
						null,
						null,
						actual.currentContext()));
				return Disposables.disposed();
			}
		}

		@Override
		public void onNext(T t) {
			if (terminated) {
				return;
			}

			if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
				Sinks.Many<T> w = window;
				w.emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);

				int c = count + 1;

				if (c >= maxSize) {
					producerIndex++;
					count = 0;

					w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);

					long r = requested;

					if (r != 0L) {
						w = Sinks.unsafe()
						         .many()
						         .unicast()
						         .onBackpressureBuffer();
						window = w;
						actual.onNext(w.asFlux());
						if (r != Long.MAX_VALUE) {
							REQUESTED.decrementAndGet(this);
						}

						Disposable tm = timer;
						tm.dispose();

						Disposable task = newPeriod();

						if (!TIMER.compareAndSet(this, tm, task)) {
							task.dispose();
						}
					}
					else {
						window = null;
						actual.onError(Operators.onOperatorError(s,
								Exceptions.failWithOverflow(),
								t,
								actual.currentContext()));
						timer.dispose();
						worker.dispose();
						return;
					}
				}
				else {
					count = c;
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
			}
			else {
				queue.offer(t);
				if (!enter()) {
					return;
				}
			}
			drainLoop();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			if (enter()) {
				drainLoop();
			}

			actual.onError(t);
			timer.dispose();
			worker.dispose();
		}

		@Override
		public void onComplete() {
			done = true;
			if (enter()) {
				drainLoop();
			}

			actual.onComplete();
			timer.dispose();
			worker.dispose();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@SuppressWarnings("unchecked")
		void drainLoop() {
			final Queue<Object> q = queue;
			final Subscriber<? super Flux<T>> a = actual;
			Sinks.Many<T> w = window;

			int missed = 1;
			for (; ; ) {

				for (; ; ) {
					if (terminated) {
						s.cancel();
						q.clear();
						timer.dispose();
						worker.dispose();
						return;
					}

					boolean d = done;

					Object o = q.poll();

					boolean empty = o == null;
					boolean isHolder = o instanceof ConsumerIndexHolder;

					if (d && (empty || isHolder)) {
						window = null;
						q.clear();
						Throwable err = error;
						if (err != null) {
							w.emitError(err, Sinks.EmitFailureHandler.FAIL_FAST);
						}
						else {
							w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
						}
						timer.dispose();
						worker.dispose();
						return;
					}

					if (empty) {
						break;
					}

					if (isHolder) {
						w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
						count = 0;
						w = Sinks.unsafe()
						         .many()
						         .unicast()
						         .onBackpressureBuffer();
						window = w;

						long r = requested;
						if (r != 0L) {
							a.onNext(w.asFlux());
							if (r != Long.MAX_VALUE) {
								REQUESTED.decrementAndGet(this);
							}
						}
						else {
							window = null;
							queue.clear();
							a.onError(Operators.onOperatorError(s,
									Exceptions.failWithOverflow(),
									actual.currentContext()));
							timer.dispose();
							worker.dispose();
							return;
						}
						continue;
					}

					w.emitNext((T) o, Sinks.EmitFailureHandler.FAIL_FAST);
					int c = count + 1;

					if (c >= maxSize) {
						producerIndex++;
						count = 0;

						w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);

						long r = requested;

						if (r != 0L) {
							w = Sinks.unsafe()
							         .many()
							         .unicast()
							         .onBackpressureBuffer();
							window = w;
							actual.onNext(w.asFlux());
							if (r != Long.MAX_VALUE) {
								REQUESTED.decrementAndGet(this);
							}

							Disposable tm = timer;
							tm.dispose();

							Disposable task = newPeriod();

							if (!TIMER.compareAndSet(this, tm, task)) {
								task.dispose();
							}
						}
						else {
							window = null;
							a.onError(Operators.onOperatorError(s,
									Exceptions.failWithOverflow(),
									o,
									actual.currentContext()));
							timer.dispose();
							worker.dispose();
							return;
						}
					}
					else {
						count = c;
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}

		static final class ConsumerIndexHolder implements Runnable {

			final long                       index;
			final WindowTimeoutSubscriber<?> parent;

			ConsumerIndexHolder(long index, WindowTimeoutSubscriber<?> parent) {
				this.index = index;
				this.parent = parent;
			}

			@Override
			public void run() {
				WindowTimeoutSubscriber<?> p = parent;

				if (!p.cancelled) {
					p.queue.offer(this);
				}
				else {
					p.terminated = true;
					p.timer.dispose();
					p.worker.dispose();
				}
				if (p.enter()) {
					p.drainLoop();
				}
			}
		}
	}

}

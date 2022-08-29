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

import java.util.Objects;
import java.util.Queue;
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
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static reactor.core.Exceptions.wrapSource;

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
			return new WindowTimeoutWithBackpressureSubscriber<>(actual, maxSize, timespan, unit, timer, null);
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

		@Nullable
		final StateLogger                     logger;

		final CoreSubscriber<? super Flux<T>> actual;
		final long                            timespan;
		final TimeUnit                        unit;
		final Scheduler                       scheduler;
		final int                             maxSize;
		final Scheduler.Worker                worker;
		final int                             limit;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutWithBackpressureSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, "requested");


		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutWithBackpressureSubscriber> STATE =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, "state");

		static final long CANCELLED_FLAG             =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TERMINATED_FLAG            =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_UNSENT_WINDOW          =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_WORK_IN_PROGRESS     =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long REQUEST_INDEX_MASK       =
				0b0000_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long ACTIVE_WINDOW_INDEX_MASK =
				0b0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000L;
		static final long NEXT_WINDOW_INDEX_MASK   =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111L;

		static final int ACTIVE_WINDOW_INDEX_SHIFT = 20;
		static final int REQUEST_INDEX_SHIFT       = 40;

		boolean done;
		Throwable error;

		Subscription s;

		InnerWindow<T> window;

		WindowTimeoutWithBackpressureSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int maxSize,
				long timespan,
				TimeUnit unit,
				Scheduler scheduler,
				@Nullable StateLogger logger) {
			this.actual = actual;
			this.timespan = timespan;
			this.unit = unit;
			this.scheduler = scheduler;
			this.maxSize = maxSize;
			this.limit = Operators.unboundedOrLimit(maxSize);
			this.worker = scheduler.createWorker();
			this.logger = logger;

			STATE.lazySet(this, 1);
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
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			while(true) {
				if (isCancelled(this.state)) {
					Operators.onDiscard(t, this.actual.currentContext());
					return;
				}

				final InnerWindow<T> window = this.window;
				if (window.sendNext(t)) {
					return;
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.error = t;
			this.done = true;

			final long previousState = markTerminated(this);

			if (isCancelled(previousState) || isTerminated(previousState)) {
				return;
			}

			final InnerWindow<T> window = this.window;
			if (window != null) {
				window.sendError(wrapSource(t));

				if (hasUnsentWindow(previousState)) {
					return;
				}
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			this.done = true;


			final long previousState = markTerminated(this);

			if (isCancelled(previousState) || isTerminated(previousState)) {
				return;
			}

			InnerWindow<T> window = this.window;
			if (window != null) {
				window.sendComplete();

				if (hasUnsentWindow(previousState)) {
					return;
				}
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			this.actual.onComplete();
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

				if (hasWorkInProgress(previousState)) {
					long nextState = (previousState & ~REQUEST_INDEX_MASK) | incrementRequestIndex(previousState);
					if (STATE.compareAndSet(this, previousState, nextState)) {
						if (this.logger != null) {
							this.logger.log(this.toString(), "mre", previousState, nextState);
						}
						return;
					}

					continue;
				}

				hasUnsentWindow = hasUnsentWindow(previousState);

				if (!hasUnsentWindow && (isTerminated(previousState) || activeWindowIndex(previousState) == nextWindowIndex(previousState))) {
					return;
				}

				expectedState =
						(previousState &~ HAS_UNSENT_WINDOW) |
						HAS_WORK_IN_PROGRESS;
				if (STATE.compareAndSet(this, previousState, expectedState)) {
					if (this.logger != null) {
						this.logger.log(this.toString(), "mre", previousState, expectedState);
					}
					break;
				}
			}

			drain(previousState, expectedState);
		}

		void tryCreateNextWindow(int windowIndex) {
			long previousState;
			long expectedState;

			for (;;) {
				previousState = this.state;

				if (isCancelled(previousState)) {
					return;
				}

				if (nextWindowIndex(previousState) != windowIndex) {
					return;
				}

				boolean hasWorkInProgress = hasWorkInProgress(previousState);
				if (!hasWorkInProgress && isTerminated(previousState) && !hasUnsentWindow(previousState)) {
					return;
				}

				expectedState = (previousState &~ NEXT_WINDOW_INDEX_MASK) | incrementNextWindowIndex(previousState) | HAS_WORK_IN_PROGRESS;
				if (STATE.compareAndSet(this, previousState, expectedState)) {

					if (hasWorkInProgress) {
						return;
					}

					break;
				}
			}

			drain(previousState, expectedState);
		}

		void drain(long previousState, long expectedState) {
			for (;;) {
				long n = this.requested;
				if (this.logger != null) {
					this.logger.log(this.toString(), "dr"+n, previousState,
							expectedState);
				}

				final boolean hasUnsentWindow = hasUnsentWindow(previousState);

				final int activeWindowIndex = activeWindowIndex(expectedState);
				final int nextWindowIndex = nextWindowIndex(expectedState);
				// short path to exit from loop if there is active window which was not
				// terminated yet and this window is delivered so we dont have to
				// perform any work (we just need to remove lock and exit)
				if (activeWindowIndex == nextWindowIndex && !hasUnsentWindow) {
					expectedState = markWorkDone(this, expectedState);
					previousState = expectedState | HAS_WORK_IN_PROGRESS;

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
						return;
					}

					continue;
				}

				if (n > 0) {
					// here we are if we entered from the request() method
					if (hasUnsentWindow) {
						final InnerWindow<T> currentUnsentWindow = this.window;

						// Delivers current unsent window
						this.actual.onNext(currentUnsentWindow);

						if (n != Long.MAX_VALUE) {
							n = REQUESTED.decrementAndGet(this);
							if (this.logger != null) {
								this.logger.log(this.toString(), "dec", n, n);
							}
						}

						// Marks as sent current unsent window. Also, delivers
						// onComplete to the window subscriber if it was not delivered
						final long previousInnerWindowState = currentUnsentWindow.sendSent();

						if (isTerminated(expectedState)) {
							Throwable e = this.error;
							if (e != null) {
								if (!isTerminated(previousInnerWindowState)) {
									currentUnsentWindow.sendError(e);
								}
								this.actual.onError(e);
							}
							else {
								if (!isTerminated(previousInnerWindowState)) {
									currentUnsentWindow.sendComplete();
								}
								this.actual.onComplete();
							}
							return;
						}

						// we should create next window if we see that
						// currentUnsentWindow is terminated and nextWindowIndex is
						// greater than currentWindowIndex
						if (nextWindowIndex > activeWindowIndex && (InnerWindow.isTimeout(previousInnerWindowState) || InnerWindow.isTerminated(previousInnerWindowState))) {
							final boolean shouldBeUnsent = n == 0;
							final InnerWindow<T> nextWindow =
									new InnerWindow<>(this.maxSize, this,
											nextWindowIndex, shouldBeUnsent, logger);

							this.window = nextWindow;

							if (!shouldBeUnsent) {
								this.actual.onNext(nextWindow);

								if (n != Long.MAX_VALUE) {
									REQUESTED.decrementAndGet(this);
								}
							}

							previousState = commitWork(this, expectedState, shouldBeUnsent);
							expectedState =
									(((previousState &~ACTIVE_WINDOW_INDEX_MASK) &~ HAS_UNSENT_WINDOW) ^ (expectedState == previousState ? HAS_WORK_IN_PROGRESS : 0)) |
									incrementActiveWindowIndex(previousState) |
									(shouldBeUnsent ? HAS_UNSENT_WINDOW : 0);
							// we need to put unsent flag here because we check at the
							// beginning of the loop the presence for unsent flag from
							// the previousState
							previousState = (previousState &~ HAS_UNSENT_WINDOW) | (shouldBeUnsent ? HAS_UNSENT_WINDOW : 0);

							if (isCancelled(expectedState)) {
								nextWindow.sendCancel();
								if (shouldBeUnsent) {
									nextWindow.cancel();
								}
								return;
							}

							if (isTerminated(expectedState) && !shouldBeUnsent) {
								final Throwable e = this.error;
								if (e != null) {
									nextWindow.sendError(e);
									this.actual.onError(e);
								}
								else {
									nextWindow.sendComplete();
									this.actual.onComplete();
								}
								return;
							}

							try {
								nextWindow.scheduleTimeout();
							}
							catch (Exception e) {
								if (hasWorkInProgress(expectedState)) {
									this.actual.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
								}
								else {
									this.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
								}

								return;
							}

							final long nextRequest = InnerWindow.received(previousInnerWindowState);
							if (nextRequest > 0) {
								this.s.request(nextRequest);
							}


							if (!hasWorkInProgress(expectedState)) {
								return;
							}
						} else {
							previousState = commitSent(this, expectedState);
							expectedState = (previousState &~ HAS_UNSENT_WINDOW) ^ (expectedState == previousState ? HAS_WORK_IN_PROGRESS : 0);
							previousState &= ~HAS_UNSENT_WINDOW;

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
								return;
							}
						}
					} else {
						final InnerWindow<T> nextWindow =
								new InnerWindow<>(this.maxSize, this, nextWindowIndex,
										false, logger);

						final InnerWindow<T> previousWindow = this.window;

						this.window = nextWindow;

						this.actual.onNext(nextWindow);

						if (n != Long.MAX_VALUE) {
							REQUESTED.decrementAndGet(this);
						}

						previousState = commitWork(this, expectedState, false);
						expectedState =
								(((previousState &~ACTIVE_WINDOW_INDEX_MASK) &~ HAS_UNSENT_WINDOW) ^ (expectedState == previousState ? HAS_WORK_IN_PROGRESS : 0)) |
										incrementActiveWindowIndex(previousState);

						if (isCancelled(expectedState)) {
							previousWindow.sendCancel();
							nextWindow.sendCancel();
							return;
						}

						if (isTerminated(expectedState)) {
							final Throwable e = this.error;
							if (e != null) {
								previousWindow.sendError(e);
								nextWindow.sendError(e);
								this.actual.onError(e);
							}
							else {
								previousWindow.sendComplete();
								nextWindow.sendComplete();
								this.actual.onComplete();
							}
							return;
						}

						try {
							nextWindow.scheduleTimeout();
						}
						catch (Exception e) {
							if (hasWorkInProgress(expectedState)) {
								this.actual.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
							}
							else {
								this.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
							}

							return;
						}

						final long nextRequest;
						if (previousWindow == null) {
							// possible at the very beginning
							nextRequest = this.maxSize;
						}
						else {
							long previousActiveWindowState = previousWindow.sendComplete();
							nextRequest = InnerWindow.received(previousActiveWindowState);
						}

						if (nextRequest > 0) {
							this.s.request(nextRequest);
						}

						if (!hasWorkInProgress(expectedState)) {
							return;
						}
					}
				}
				else if (n == 0 && !hasUnsentWindow) {
					final InnerWindow<T> nextWindow =
							new InnerWindow<>(this.maxSize, this, nextWindowIndex, true, logger);

					final InnerWindow<T> previousWindow = this.window;
					this.window = nextWindow;

					// doesn't propagate through onNext since window is unsent

					previousState = commitWork(this, expectedState, true);
					expectedState =
							(((previousState &~ACTIVE_WINDOW_INDEX_MASK) &~ HAS_UNSENT_WINDOW) ^ (expectedState == previousState ? HAS_WORK_IN_PROGRESS : 0)) |
									incrementActiveWindowIndex(previousState) |
									HAS_UNSENT_WINDOW;
					previousState |= HAS_UNSENT_WINDOW;

					if (isCancelled(expectedState)) {
						previousWindow.sendCancel();
						nextWindow.sendCancel();
						nextWindow.cancel();
						return;
					}

					// window is deliberately unsent, we can not deliver it since no
					// demand even if it is terminated

					try {
						nextWindow.scheduleTimeout();
					}
					catch (Exception e) {
						if (hasWorkInProgress(expectedState)) {
							this.actual.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
						}
						else {
							this.onError(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
						}

						return;
					}

					long previousActiveWindowState = previousWindow.sendComplete();
					final long nextRequest = InnerWindow.received(previousActiveWindowState);

					if (nextRequest > 0) {
						this.s.request(nextRequest);
					}

					if (!hasWorkInProgress(expectedState)) {
						return;
					}
				}
				else {
					expectedState = markWorkDone(this, expectedState);
					previousState = expectedState | HAS_WORK_IN_PROGRESS;

					if (isCancelled(expectedState)) {
						final InnerWindow<T> currentWindow = this.window;
						final long previousWindowState = currentWindow.sendCancel();
						if (!InnerWindow.isSent(previousWindowState)) {
							currentWindow.cancel();
						}
						return;
					}

					if (isTerminated(expectedState) && !hasUnsentWindow(expectedState)) {
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
				}
			}
		}

		@Override
		public void cancel() {
			final long previousState = markCancelled(this);
			if ((!hasWorkInProgress(previousState) && isTerminated(previousState) && !hasUnsentWindow(previousState)) || isCancelled(previousState)) {
				return;
			}

			this.s.cancel();

			final InnerWindow<T> currentActiveWindow = this.window;
			if (currentActiveWindow != null) {
				if (!InnerWindow.isSent(currentActiveWindow.sendCancel())) {
					if (!hasWorkInProgress(previousState)) {
						currentActiveWindow.cancel();
					}
				}
			}
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

		static boolean hasUnsentWindow(long state) {
			return (state & HAS_UNSENT_WINDOW) == HAS_UNSENT_WINDOW;
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}

		static boolean hasWorkInProgress(long state) {
			return (state & HAS_WORK_IN_PROGRESS) == HAS_WORK_IN_PROGRESS;
		}

		static long incrementRequestIndex(long state) {
			return ((((state & REQUEST_INDEX_MASK) >> REQUEST_INDEX_SHIFT) + 1) << REQUEST_INDEX_SHIFT) & REQUEST_INDEX_MASK;
		}

		static long incrementActiveWindowIndex(long state) {
			return ((((state & ACTIVE_WINDOW_INDEX_MASK) >> ACTIVE_WINDOW_INDEX_SHIFT) + 1) << ACTIVE_WINDOW_INDEX_SHIFT) & ACTIVE_WINDOW_INDEX_MASK;
		}

		static int activeWindowIndex(long state) {
			return (int) ((state & ACTIVE_WINDOW_INDEX_MASK) >> ACTIVE_WINDOW_INDEX_SHIFT);
		}

		static long incrementNextWindowIndex(long state) {
			return ((state & NEXT_WINDOW_INDEX_MASK) + 1) & NEXT_WINDOW_INDEX_MASK;
		}

		static int nextWindowIndex(long state) {
			return (int) (state & NEXT_WINDOW_INDEX_MASK);
		}



		/**
		 * Adds {@link #TERMINATED_FLAG} to indicate cancellation fact. Operation fails
		 * if current state is already terminated or cancelled
		 *
		 * @param instance from which to read state
		 *
		 * @return previous state
		 */
		static <T> long markTerminated(WindowTimeoutWithBackpressureSubscriber<T> instance) {
			for(;;) {
				final long previousState = instance.state;

				if (isTerminated(previousState) || isCancelled(previousState)) {
					return previousState;
				}

				final long nextState = previousState | TERMINATED_FLAG;
				if (STATE.compareAndSet(instance, previousState, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mtd", previousState, nextState);
					}
					return previousState;
				}
			}
		}

		/**
		 * Adds {@link #CANCELLED_FLAG} to indicate cancellation fact. Operation fails
		 * if current state is already cancelled, or it has no work-in-progress and
		 * is-terminated and has no unsent window
		 *
		 * @param instance from which to read state
		 *
		 * @return previous state
		 */
		static <T> long markCancelled(WindowTimeoutWithBackpressureSubscriber<T> instance) {
			for (;;) {
				final long previousState = instance.state;
				if ((!hasWorkInProgress(previousState) && isTerminated(previousState) && !hasUnsentWindow(previousState)) || isCancelled(previousState)) {
					return previousState;
				}

				final long nextState = previousState | CANCELLED_FLAG;
				if (STATE.compareAndSet(instance, previousState, nextState)) {
					return previousState;
				}
			}
		}

		/**
		 * Removes {@link #HAS_WORK_IN_PROGRESS} to indicate no work-in-progress.
		 * Operation fails if current state does not equeal to the expected one
		 *
		 * @param instance from which to read state
		 *
		 * @return current state if fail or next state if successfully applied
		 */
		static <T> long markWorkDone(WindowTimeoutWithBackpressureSubscriber<T> instance, long expectedState) {
			for (;;) {
				final long currentState = instance.state;

				if (expectedState != currentState) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "fwd", currentState, currentState);
					}
					return currentState;
				}

				final long nextState = currentState ^ HAS_WORK_IN_PROGRESS;
				if (STATE.compareAndSet(instance, currentState, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mwd", currentState, nextState);
					}
					return nextState;
				}
			}
		}

		/**
		 * Commits fact that an unsent window is delivered. This
		 * operation tries to remove {@link #HAS_WORK_IN_PROGRESS} flag if current state
		 * is equal to the expected one.
		 *
		 * @param instance from which to read state
		 *
		 * @return previous state
		 */
		static <T> long commitSent(WindowTimeoutWithBackpressureSubscriber<T> instance, long expectedState) {
			for (;;) {
				final long currentState = instance.state;

				final long clearState = (currentState &~ HAS_UNSENT_WINDOW);
				final long nextState = (clearState ^ (expectedState == currentState ? HAS_WORK_IN_PROGRESS : 0));

				if (STATE.compareAndSet(instance, currentState, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "cts", currentState, nextState);
					}
					return currentState;
				}
			}
		}

		/**
		 * Commits new active window index and removes {@link #HAS_UNSENT_WINDOW} if
		 * specified. This operation tries to remove {@link #HAS_WORK_IN_PROGRESS} flag
		 * if current state is equal to the expected one.
		 *
		 * @param instance from which to read state
		 *
		 * @return previous state
		 */
		static <T> long commitWork(WindowTimeoutWithBackpressureSubscriber<T> instance, long expectedState, boolean setUnsentFlag) {
			for (;;) {
				final long currentState = instance.state;

				final long clearState = ((currentState &~ACTIVE_WINDOW_INDEX_MASK) &~ HAS_UNSENT_WINDOW);
				final long nextState = (clearState ^ (expectedState == currentState ? HAS_WORK_IN_PROGRESS : 0)) |
						incrementActiveWindowIndex(currentState) |
						(setUnsentFlag ? HAS_UNSENT_WINDOW : 0);

				if (STATE.compareAndSet(instance, currentState, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "ctw", currentState, nextState);
					}
					return currentState;
				}
			}
		}
	}

	static final class InnerWindow<T> extends Flux<T>
			implements InnerProducer<T>, Runnable {

		static final Disposable DISPOSED = Disposables.disposed();


		@Nullable
		final StateLogger                                logger;

		final WindowTimeoutWithBackpressureSubscriber<T> parent;
		final int                                        max;
		final Queue<T>                                   queue;
		final long                                       createTime;
		final int                                        index;

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

		int received = 0;
		int produced = 0;

		 InnerWindow(
				int max,
				WindowTimeoutWithBackpressureSubscriber<T> parent,
				int index,
				boolean markUnsent,
				@Nullable StateLogger logger) {
			this.max = max;
			this.parent = parent;
			this.queue = Queues.<T>get(max).get();
			this.index = index;
			this.logger = logger;

			 if (markUnsent) {
				STATE.lazySet(this, UNSENT_STATE);
				if (this.logger != null) {
					this.logger.log(this.toString(), "mct", 0, UNSENT_STATE);
				}
			} else {
				if (this.logger != null) {
					this.logger.log(this.toString(), "mct", 0, 0);
				}
			}

			this.createTime = parent.now();
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> actual) {
			long previousState = markSubscribedOnce(this);
			if (hasSubscribedOnce(previousState)) {
				Operators.error(actual, new IllegalStateException("Only one subscriber allowed"));
				return;
			}

			this.actual = actual;

			actual.onSubscribe(this);

			previousState = markSubscriberSet(this);
			if (isFinalized(previousState) || hasWorkInProgress(previousState)) {
				return;
			}

			if (!hasValues(previousState) && isTerminated(previousState)) {
				final Throwable t = this.error;
				if (t != null) {
					actual.onError(t);
				} else {
					actual.onComplete();
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public void request(long n) {
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

		/**
		 * This method is called from the parent when the parent is cancelled. We don't
		 * send any exceptions to the subscriber here but we deliver complete window
		 * and discard all further values delivered through the {@link #sendNext} method
		 *
		 * @return previous state
		 */
		long sendCancel() {
			for (;;) {
				final long state = this.state;

				if (isCancelledByParent(state)) {
					return state;
				}

				final long cleanState = state & ~WORK_IN_PROGRESS_MAX;
				final long nextState = cleanState |
						TERMINATED_STATE |
						PARENT_CANCELLED_STATE |
						(
							hasSubscriberSet(state)
								? hasValues(state)
									? incrementWork(state & WORK_IN_PROGRESS_MAX)
									: FINALIZED_STATE
								: 0
						);

				if (STATE.compareAndSet(this, state, nextState)) {

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
			final int received = this.received + 1 ;
			if (received > this.max) {
				return false;
			}
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

					if (!isCancelledByParent(previousState)) {
						this.parent.tryCreateNextWindow(this.index);
					}
				}
			} else {
				previousState = markHasValues(this);
				nextState = (previousState | HAS_VALUES_STATE) + 1;
			}

			if (isFinalized(previousState)) {
				if (isCancelledByParent(previousState)) {
					clearQueue();
					return true;
				}
				else if (isCancelled(previousState)) {
					clearQueue();
					// doing extra request since index progress was not committed but
					// value is discarded
					this.parent.s.request(1);
					return true;
				}
				else {
					if (this.queue.poll() != t) {
						// doing extra request since the value is sent event though the
						// index progress was not committed
						this.parent.s.request(1);
						return true;
					}
					else {
						return false;
					}
				}
			}

			if (isTimeout(previousState) && isTerminated(previousState)) {
				// doing extra request since the value being sent is not replenished
				this.parent.s.request(1);
			}

			if (hasSubscriberSet(previousState)) {
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

		long sendComplete() {
			final long previousState = markTerminated(this);
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
				if (hasWorkInProgress(previousState)) {
					return previousState;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return previousState;
				}

				if (hasValues(previousState)) {
					drain((previousState | TERMINATED_STATE) + 1);
				} else {
					this.actual.onComplete();
				}
			}

			return previousState;
		}

		long sendError(Throwable error) {
			 this.error = error;

			final long previousState = markTerminated(this);
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
				if (hasWorkInProgress(previousState)) {
					return previousState;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return previousState;
				}

				if (hasValues(previousState)) {
					drain((previousState | TERMINATED_STATE) + 1);
				} else {
					this.actual.onError(error);
				}
			}

			return previousState;
		}

		long sendSent() {
			final long previousState = markSent(this);
			if (isFinalized(previousState) || !isTerminated(previousState) && !isTimeout(previousState)) {
				return previousState;
			}

			if (hasSubscriberSet(previousState)) {
				if (hasWorkInProgress(previousState)) {
					return previousState;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return previousState;
				}

				if (hasValues(previousState)) {
					drain(((previousState ^ UNSENT_STATE) | TERMINATED_STATE) + 1);
				} else {
					this.actual.onComplete();
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
					if (checkTerminated((this.produced + e), a, v)) {
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

			if (value == null && received(state) <= totalProduced && isTerminated(state)) {
				if (!markFinalized(state)) {
					return false;
				}

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
			if (isTerminated(previousState) || isCancelledByParent(previousState)) {
				return;
			}

			this.parent.tryCreateNextWindow(this.index);
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
					return;
				}
			}
		}

		boolean markFinalized(long state) {
			final long nextState = ((state | FINALIZED_STATE) & ~WORK_IN_PROGRESS_MAX) ^ (
					hasValues(state) ? HAS_VALUES_STATE : 0);
			if (STATE.compareAndSet(this, state, nextState)) {
				return true;
			}

			return false;
		}

		void clearQueue() {
			final Queue<T> q = this.queue;
			final Context context = this.actual != null ? this.actual.currentContext() : this.parent.currentContext();

			T v;
			while ((v = q.poll()) != null) {
				Operators.onDiscard(v, context);
			}
		}

		static final long FINALIZED_STATE          =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TERMINATED_STATE         =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long PARENT_CANCELLED_STATE   =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long CANCELLED_STATE          =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TIMEOUT_STATE            =
				0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_VALUES_STATE         =
				0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_STATE     =
				0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_SET_STATE =
				0b0000_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long UNSENT_STATE             =
				0b0000_0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long RECEIVED_MASK            =
				0b0000_0000_0111_1111_1111_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000_0000L;
		static final long WORK_IN_PROGRESS_MAX     =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_1111L;
		static final long RECEIVED_SHIFT_BITS      = 24;

		static <T> long markSent(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long nextState =
						(state ^ UNSENT_STATE) |
						((isTimeout(state) || isTerminated(state))
								? TERMINATED_STATE | (
										hasSubscriberSet(state)
												? hasValues(state)
													? incrementWork(state & WORK_IN_PROGRESS_MAX)
	                                                : FINALIZED_STATE
												: 0
									)
								: 0);
				if (STATE.compareAndSet(instance, state, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mst", state,
								nextState);
					}
					return state;
				}
			}
		}

		static boolean isSent(long state) {
			return (state & UNSENT_STATE) == 0;
		}

		static <T> long markTimeout(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isTerminated(state)) {
					return state;
				}

				final long nextState = state | TIMEOUT_STATE;
				if (STATE.compareAndSet(instance, state, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mtt", state, nextState);
					}
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
						cleanState | CANCELLED_STATE | HAS_SUBSCRIBER_SET_STATE | incrementWork(state & WORK_IN_PROGRESS_MAX);
				if (STATE.compareAndSet(instance, state, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mcd", state,
								nextState);
					}
					return state;
				}
			}
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_STATE) == CANCELLED_STATE;
		}

		static boolean isCancelledByParent(long state) {
			return (state & PARENT_CANCELLED_STATE) == PARENT_CANCELLED_STATE;
		}

		static <T> long markHasValues(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isFinalized(state)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "fhv", state, state);
					}
					return state;
				}

				final long nextState;
				if (hasSubscriberSet(state)) {
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
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mhv", state,
								nextState);
					}
					return state;
				}
			}
		}

		static <T> long markHasValuesAndTerminated(InnerWindow<T> instance) {
			for (;;) {
				final long state = instance.state;

				if (isFinalized(state)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "fht", state, state);
					}
					return state;
				}

				final long cleanState =
						(state & ~WORK_IN_PROGRESS_MAX) & ~RECEIVED_MASK;
				final long nextState;
				if (hasSubscriberSet(state)) {
					nextState = cleanState |
							HAS_VALUES_STATE |
							TERMINATED_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							incrementWork(state & WORK_IN_PROGRESS_MAX);
				}
				else {
					nextState = cleanState |
							HAS_VALUES_STATE |
							TERMINATED_STATE |
							incrementReceived(state & RECEIVED_MASK) |
							(hasWorkInProgress(state) ? incrementWork(state & WORK_IN_PROGRESS_MAX) : 0);
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "hvt", state,
								nextState);
					}
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
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mhr", state, nextState);
					}
					return state;
				}
			}
		}

		static <T> long markTerminated(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isFinalized(state) || isTerminated(state)) {
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
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mtd", state, nextState);
					}
					return state;
				}
			}
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_STATE) == TERMINATED_STATE;
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
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mso", state, nextState);
					}
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
						(state | HAS_SUBSCRIBER_SET_STATE) | (isTerminated(state) && !hasValues(state) ? FINALIZED_STATE : 0);
				if (STATE.compareAndSet(instance, state, nextState)) {
					if (instance.logger != null) {
						instance.logger.log(instance.toString(), "mss", state, nextState);
					}
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
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "mwd", state, nextState);
				}
				return nextState;
			}

			return instance.state;
		}

		static boolean hasWorkInProgress(long state) {
			return (state & WORK_IN_PROGRESS_MAX) > 0;
		}

		static boolean isFinalized(long state) {
			return (state & FINALIZED_STATE) == FINALIZED_STATE;
		}

		@Override
		public String toString() {
			return super.toString() + " " + index;
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

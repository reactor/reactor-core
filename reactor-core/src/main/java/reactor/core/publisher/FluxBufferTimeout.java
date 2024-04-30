/*
 * Copyright (c) 2016-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class FluxBufferTimeout<T, C extends Collection<? super T>> extends InternalFluxOperator<T, C> {

	final int         batchSize;
	final Supplier<C> bufferSupplier;
	final Scheduler   timer;
	final long        timespan;
	final TimeUnit    unit;
	final boolean     fairBackpressure;
	final Logger      logger;

	FluxBufferTimeout(Flux<T> source,
			int maxSize,
			long timespan,
			TimeUnit unit,
			Scheduler timer,
			Supplier<C> bufferSupplier,
			boolean fairBackpressure) {
		super(source);
		if (timespan <= 0) {
			throw new IllegalArgumentException("Timeout period must be strictly positive");
		}
		if (maxSize <= 0) {
			throw new IllegalArgumentException("maxSize must be strictly positive");
		}
		this.timer = Objects.requireNonNull(timer, "Timer");
		this.timespan = timespan;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.batchSize = maxSize;
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.fairBackpressure = fairBackpressure;
		this.logger = null;
	}

	// for testing
	FluxBufferTimeout(Flux<T> source,
			int maxSize,
			long timespan,
			TimeUnit unit,
			Scheduler timer,
			Supplier<C> bufferSupplier,
			boolean fairBackpressure,
			Logger logger) {
		super(source);
		if (timespan <= 0) {
			throw new IllegalArgumentException("Timeout period must be strictly positive");
		}
		if (maxSize <= 0) {
			throw new IllegalArgumentException("maxSize must be strictly positive");
		}
		this.timer = Objects.requireNonNull(timer, "Timer");
		this.timespan = timespan;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.batchSize = maxSize;
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.fairBackpressure = fairBackpressure;
		this.logger = logger;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		if (fairBackpressure) {
			return new BufferTimeoutWithBackpressureSubscriber<>(actual,
					batchSize,
					timespan,
					unit,
					timer.createWorker(),
					bufferSupplier,
					null);
		}
		return new BufferTimeoutSubscriber<>(
				Operators.serialize(actual),
				batchSize,
				timespan,
				unit,
				timer.createWorker(),
				bufferSupplier
		);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timer;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	final static class BufferTimeoutWithBackpressureSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final @Nullable Logger                    logger;
		final @Nullable StateLogger               stateLogger;
		final           CoreSubscriber<? super C> actual;
		final           int                       batchSize;
		final           int                       prefetch;
		final           long                      timeSpan;
		final           TimeUnit                  unit;
		final           Scheduler.Worker          timer;
		final           Supplier<C>               bufferSupplier;
		private final   Disposable.Swap           currentTimeoutTask = Disposables.swap();

		private @Nullable Subscription subscription;
		private           Queue<T>     queue;

		private @Nullable Throwable error;
		private           boolean   done;

		// tracks unsatisfied downstream demand (expressed in # of buffers)
		volatile long requested;
		@SuppressWarnings("rawtypes")
		private AtomicLongFieldUpdater<BufferTimeoutWithBackpressureSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "requested");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferTimeoutWithBackpressureSubscriber> STATE =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "state");

		static final long CANCELLED_FLAG            =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TERMINATED_FLAG           =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_WORK_IN_PROGRESS_FLAG =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TIMEOUT_FLAG              =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long REQUESTED_INDEX_MASK      =
				0b0000_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long OUTSTANDING_MASK          =
				0b0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000L;
		static final long INDEX_MASK                =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111L;

		static final int INDEX_SHIFT = 0;
		static final int OUTSTANDING_SHIFT = 20;
		static final int REQUESTED_INDEX_SHIFT = 40;
		static final int INDEX_LIMIT = 1 << OUTSTANDING_SHIFT; // 1048576; // 2^20

		public BufferTimeoutWithBackpressureSubscriber(
				CoreSubscriber<? super C> actual,
				int batchSize,
				long timeSpan,
				TimeUnit unit,
				Scheduler.Worker timer,
				Supplier<C> bufferSupplier,
				@Nullable Logger logger) {
			this.actual = actual;
			// TODO: reconsider OUTSTANDING to be taken out of the mask to allow for higher value
			//  -> this translates to 4MiB of ints
			if (batchSize >= INDEX_LIMIT) {
				throw new IllegalArgumentException("Batch size can't exceed " + INDEX_LIMIT + " items");
			}
			this.batchSize = batchSize;
			this.timeSpan = timeSpan;
			this.unit = unit;
			this.timer = timer;
			this.bufferSupplier = bufferSupplier;
			this.logger = logger;
			this.stateLogger = logger != null ? new StateLogger(logger) : null;
			this.prefetch = batchSize << 2;
			this.queue = Queues.<T>get(prefetch).get();
		}

		private void trace(Logger logger, String msg) {
			logger.trace(String.format("[%s][%s]", Thread.currentThread().getId(), msg));
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.subscription, s)) {
				this.subscription = s;
				this.actual.onSubscribe(this);
			}
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return this.actual;
		}

		@Override
		public void request(long n) {
			if (logger != null) {
				trace(logger, "request " + n);
			}
			if (Operators.validate(n)) {
				long previouslyRequested = Operators.addCap(REQUESTED, this, n);
				if (previouslyRequested == Long.MAX_VALUE) {
					return;
				}

				long previousState;
				for (;;) {
					previousState = this.state;

					if (queue.isEmpty() && (isTerminated(previousState) || isCancelled(previousState))) {
						return;
					}

					if (hasWorkInProgress(previousState)) {
						// We let the active worker know about a change in demand.
						long nextState = incrementRequestIndex(previousState);
						if (STATE.compareAndSet(this, previousState, nextState)) {
							if (this.stateLogger != null) {
								this.stateLogger.log(this.toString(), "req", previousState,	nextState);
							}
							// If the state was replaced but there was work in progress,
							// before leaving the protected section the working actor
							// will notice an update and loop again to pick up the
							// demand increase. If the active actor was done before our
							// update, this update would have failed and we'd loop
							// again to see the current state.
							return;
						}
						// CAS failed, retry
						continue;
					}

					long nextState = previousState | HAS_WORK_IN_PROGRESS_FLAG;

					if (STATE.compareAndSet(this, previousState, nextState)) {
						if (this.stateLogger != null) {
							this.stateLogger.log(this.toString(), "req", previousState, nextState);
						}
						break;
					}
				}

				// if there was no demand before - try to fulfill the demand if there
				// are buffered values
				drain(previouslyRequested == 0);
			}
		}

		@Override
		public void onNext(T t) {
			if (logger != null) {
				trace(logger, "onNext " + t);
			}
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			boolean enqueued = queue.offer(t);
			if (!enqueued) {
				this.error = Operators.onOperatorError(
						this.subscription,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t,
						this.actual.currentContext());
				Operators.onDiscard(t, this.actual.currentContext());
			}
			long previousState = this.state;

			if (enqueued) {
				// Only onNext increments the index. Drain can set it to 0 when it
				// flushes. However, timeout does not reset it to 0, it has its own
				// flag.
				boolean terminated = false;
				if (terminated) {
					previousState = forceAddWork(this, BufferTimeoutWithBackpressureSubscriber::setTerminated);
				} else {
					previousState = forceAddWork(this, state -> incrementIndex(state, 1));
				}
				// We can only fire the timer once we increment the index first so that
				// the timer doesn't fire first as it would consume the element and try
				// to decrement the index below 0.
				if (getIndex(previousState) == 0) {
					// fire timer, new buffer starts
					try {
						Disposable disposable =
								timer.schedule(this::bufferTimedOut, timeSpan, unit);
						currentTimeoutTask.update(disposable);
					} catch (RejectedExecutionException e) {
						this.error = Operators.onRejectedExecution(e, subscription, null, t, actual.currentContext());
						terminated = true;
					}
				}
			} else {
				previousState = forceAddWork(this, BufferTimeoutWithBackpressureSubscriber::setTerminated);
			}

			if (!hasWorkInProgress(previousState)) {
				drain(false);
			}
		}

		void bufferTimedOut() {
			if (logger != null) {
				trace(logger, "timedOut");
			}
			if (this.done) {
				return;
			}

			long previousState = forceAddWork(this, BufferTimeoutWithBackpressureSubscriber::setTimedOut);

			if (!hasWorkInProgress(previousState)) {
				drain(false);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (logger != null) {
				trace(logger, "onError " + t);
			}
			if (this.done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			this.error = t;
			long previousState = forceAddWork(this,
					BufferTimeoutWithBackpressureSubscriber::setTerminated);

			if (!hasWorkInProgress(previousState)) {
				drain(false);
			}
		}

		@Override
		public void onComplete() {
			if (logger != null) {
				trace(logger, "onComplete");
			}
			if (this.done) {
				return;
			}

			long previousState = forceAddWork(this, BufferTimeoutWithBackpressureSubscriber::setTerminated);

			if (!hasWorkInProgress(previousState)) {
				drain(false);
			}
		}

		@Override
		public void cancel() {
			if (logger != null) {
				trace(logger, "cancel");
			}
			if (this.done || isCancelled(this.state)) {
				return;
			}

			if (this.subscription != null) {
				subscription.cancel();
			}

			long previousState = forceAddWork(this, BufferTimeoutWithBackpressureSubscriber::setCancelled);

			if (!hasWorkInProgress(previousState)) {
				drain(false);
			}
		}

		// Draining doesn't ensure exclusive access - the caller is responsible for
		// ensuring that.
		private void drain(boolean resumeDemand) {
			for (;;) {
				if (logger != null) {
					trace(logger, "drain start");
				}
				long previousState = this.state;
				long currentState = previousState;

				if (done || isCancelled(currentState)) {
					if (logger != null) {
						trace(logger, "Discarding entire queue of " + queue.size());
					}
					Operators.onDiscardQueueWithClear(queue, currentContext(), null);
					currentState = tryClearWip(this, currentState);
					if (!hasWorkInProgress(currentState)) {
						return;
					}
				} else {
					long index = getIndex(currentState);
					long currentRequest = this.requested;
					boolean shouldFlush = currentRequest > 0
							&& (resumeDemand || isTimedOut(currentState) || isTerminated(currentState) || index >= batchSize);

					int consumed = 0;
					if (logger != null) {
						trace(logger, "should flush: " + shouldFlush + " currentRequest: " + currentRequest + " index: " + index + " isTerminated: " + isTerminated(currentState) + " isTimedOut: " + isTimedOut(currentState));
					}
					if (shouldFlush) {
						currentTimeoutTask.update(null);
						for (; ; ) {
							int consumedNow = flush();
							if (logger != null) {
								trace(logger, "flushed: " + consumedNow);
							}
							// Need to make sure that if work is added we clear the
							// resumeDemand with which we entered the drain loop as the
							// state is now different
							resumeDemand = false;
							if (consumedNow == 0) {
								break;
							}
							consumed += consumedNow;
							if (currentRequest != Long.MAX_VALUE) {
								currentRequest = REQUESTED.decrementAndGet(this);
							}
							if (currentRequest == 0) {
								break;
							}
						}

					}

					boolean terminated = isTerminated(currentState);

					if (consumed > 0) {
//							currentState = addOutstanding(this, -consumed);
						long decrement = -consumed;
						currentState = forceUpdate(this, state -> addOutstanding(state, decrement));
						previousState = addOutstanding(previousState, decrement);
					}
					if (!terminated && currentRequest > 0) {
						// request more from upstream
						int remaining = getOutstanding(currentState);
						int replenishMark = prefetch >> 1; // TODO: create field limit instead
						if (remaining < replenishMark) {
							currentState = requestMore(prefetch - remaining);
							previousState = addOutstanding(previousState, prefetch - remaining);
						}
					}

					if (terminated && queue.isEmpty()) {
						done = true;
						if (logger != null) {
							trace(logger, "terminated! error: " + this.error + " queue size: " + queue.size());
						}
						if (this.error != null) {
							Operators.onDiscardQueueWithClear(queue, currentContext(), null);
							actual.onError(this.error);
						} else if (queue.isEmpty()) {
							actual.onComplete();
						}
					}

					if (consumed > 0) {
						int toDecrement = -consumed;
						currentState = forceUpdate(this, state -> resetTimeout(incrementIndex(state, toDecrement)));
						previousState = resetTimeout(incrementIndex(previousState, toDecrement));
					}

					currentState = tryClearWip(this, previousState);

					// If the state changed (e.g. new item arrived, a request was issued,
					// cancellation, error, completion) we will loop again.
					if (!hasWorkInProgress(currentState)) {
						if (logger != null) {
							trace(logger, "drain done");
						}
						return;
					}
					if (logger != null) {
						trace(logger, "drain repeat");
					}
				}
			}
		}

		int flush() {
			T element;
			C buffer;

			element = queue.poll();
			if (element == null) {
				// there is demand, but queue is empty
				return 0;
			}
			buffer = bufferSupplier.get();
			int i = 0;
			do {
				buffer.add(element);
			} while ((++i < batchSize) && ((element = queue.poll()) != null));

			actual.onNext(buffer);

			return i;
		}

		private long requestMore(int n) {
			if (logger != null) {
				trace(logger, "requestMore " + n);
			}
			long currentState = forceUpdate(this, state -> addOutstanding(state, n));
			Objects.requireNonNull(this.subscription).request(n);
			return currentState;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return this.subscription;
			if (key == Attr.CANCELLED) return isCancelled(this.state);
			if (key == Attr.TERMINATED) return isTerminated(this.state);
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CAPACITY) return prefetch; // TODO: revise
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.RUN_ON) return timer;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		private static long bitwiseInc(long state, long mask, long shift, int amount) {
			long shiftAndAdd = ((state & mask) >> shift) + amount;
			long shiftBackAndLimit = (shiftAndAdd << shift) & mask;
			long clearedState = state & ~mask;
			return clearedState | shiftBackAndLimit;
		}

		private static boolean isTerminated(long state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}

		private static long setTerminated(long state) {
			return state | TERMINATED_FLAG;
		}

		private static boolean isCancelled(long state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		private static long setCancelled(long state) {
			return state | CANCELLED_FLAG;
		}

		private static long incrementRequestIndex(long state) {
			return bitwiseInc(state, REQUESTED_INDEX_MASK, REQUESTED_INDEX_SHIFT, 1);
		}

		private static long getIndex(long state) {
			return (state & INDEX_MASK) >> INDEX_SHIFT;
		}

		private static long incrementIndex(long state, int amount) {
			return bitwiseInc(state, INDEX_MASK, INDEX_SHIFT, amount);
		}

		private static boolean hasWorkInProgress(long state) {
			return (state & HAS_WORK_IN_PROGRESS_FLAG) == HAS_WORK_IN_PROGRESS_FLAG;
		}

		private static long setWorkInProgress(long state) {
			return state | HAS_WORK_IN_PROGRESS_FLAG;
		}

		private static long setTimedOut(long state) {
			return state | TIMEOUT_FLAG;
		}

		private static long resetTimeout(long state) {
			return state & ~TIMEOUT_FLAG;
		}

		private static boolean isTimedOut(long state) {
			return (state & TIMEOUT_FLAG) == TIMEOUT_FLAG;
		}

		private static long forceAddWork(BufferTimeoutWithBackpressureSubscriber<?, ?> instance, Function<Long, Long> f) {
			for (;;) {
				long previousState = instance.state;
				long nextState = f.apply(previousState) | HAS_WORK_IN_PROGRESS_FLAG;
				if (STATE.compareAndSet(instance, previousState, nextState)) {
					if (instance.stateLogger != null) {
						instance.stateLogger.log(instance.toString(),
								"faw",
								previousState,
								nextState);
					}
					return previousState;
				}
			}
		}

		private static long forceUpdate(BufferTimeoutWithBackpressureSubscriber<?, ?> instance, Function<Long, Long> f) {
			for (;;) {
				long previousState = instance.state;
				long nextState = f.apply(previousState);
				if (STATE.compareAndSet(instance, previousState, nextState)) {
					if (instance.stateLogger != null) {
						instance.stateLogger.log(instance.toString(),
								"fup",
								previousState,
								nextState);
					}
					return nextState;
				}
			}
		}

		private static int getOutstanding(long state) {
			return (int) ((state & OUTSTANDING_MASK) >> OUTSTANDING_SHIFT);
		}

		private static long addOutstanding(long state, long amount) {
			long previousWithOutstandingClear = state &~ OUTSTANDING_MASK;
			long outstandingMax = OUTSTANDING_MASK >> OUTSTANDING_SHIFT;
			long current = (state & OUTSTANDING_MASK) >> OUTSTANDING_SHIFT;
			long added = Math.min(current + amount, outstandingMax);
			long newOutstanding = ((long) added) << OUTSTANDING_SHIFT;
			return previousWithOutstandingClear | newOutstanding;
		}

		private static <T, C extends Collection<? super T>> long tryClearWip(BufferTimeoutWithBackpressureSubscriber<T, C> instance, long expectedState) {
			for (;;) {
				final long currentState = instance.state;

				if (expectedState != currentState) {
					return currentState;
				}

				// remove both WIP and requested_index so that we avoid overflowing
				long nextState = currentState & ~HAS_WORK_IN_PROGRESS_FLAG & ~REQUESTED_INDEX_MASK;
				if (STATE.compareAndSet(instance, currentState, nextState)) {
					if (instance.stateLogger != null) {
						instance.stateLogger.log(instance.toString(),
								"wcl",
								currentState,
								nextState);
					}
					return nextState;
				}
			}
		}
	}

	final static class BufferTimeoutSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final CoreSubscriber<? super C> actual;

		final static int NOT_TERMINATED          = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR   = 2;
		final static int TERMINATED_WITH_CANCEL  = 3;

		final int                        batchSize;
		final long                       timespan;
		final TimeUnit                   unit;
		final Scheduler.Worker           timer;
		final Runnable                   flushTask;

		protected Subscription subscription;

		volatile     int                                                  terminated =
				NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferTimeoutSubscriber> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "terminated");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferTimeoutSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "requested");

		volatile long outstanding;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferTimeoutSubscriber> OUTSTANDING =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "outstanding");

		volatile int index = 0;

		static final AtomicIntegerFieldUpdater<BufferTimeoutSubscriber> INDEX =
				AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "index");


		volatile Disposable timespanRegistration;

		final Supplier<C> bufferSupplier;

		volatile C values;

		BufferTimeoutSubscriber(CoreSubscriber<? super C> actual,
				int maxSize,
				long timespan,
				TimeUnit unit,
				Scheduler.Worker timer,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.timespan = timespan;
			this.unit = unit;
			this.timer = timer;
			this.flushTask = () -> {
				if (terminated == NOT_TERMINATED) {
					int index;
					for(;;){
						index = this.index;
						if(index == 0){
							return;
						}
						if(INDEX.compareAndSet(this, index, 0)){
							break;
						}
					}
					flushCallback(null);
				}
			};

			this.batchSize = maxSize;
			this.bufferSupplier = bufferSupplier;
		}

		protected void doOnSubscribe() {
			values = bufferSupplier.get();
		}

		void nextCallback(T value) {
			synchronized (this) {
				if (OUTSTANDING.decrementAndGet(this) < 0)
				{
					actual.onError(Exceptions.failWithOverflow("Unrequested element received"));
					Context ctx = actual.currentContext();
					Operators.onDiscard(value, ctx);
					Operators.onDiscardMultiple(values, ctx);
					return;
				}

				C v = values;
				if(v == null) {
					v = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
					values = v;
				}
				v.add(value);
			}
		}

		void flushCallback(@Nullable T ev) { //TODO investigate ev not used
			final C v;
			boolean flush = false;
			synchronized (this) {
				v = values;
				if (v != null && !v.isEmpty()) {
					values = bufferSupplier.get();
					flush = true;
				}
			}

			if (flush) {
				long r = requested;
				if (r != 0L) {
					if (r != Long.MAX_VALUE) {
						long next;
						for (;;) {
							next = r - 1;
							if (REQUESTED.compareAndSet(this, r, next)) {
								actual.onNext(v);
								return;
							}

							r = requested;
							if (r <= 0L) {
								break;
							}
						}
					}
					else {
						actual.onNext(v);
						return;
					}
				}

				cancel();
				actual.onError(Exceptions.failWithOverflow(
						"Could not emit buffer due to lack of requests"));
				Operators.onDiscardMultiple(v, this.actual.currentContext());
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.CANCELLED) return terminated == TERMINATED_WITH_CANCEL;
			if (key == Attr.TERMINATED) return terminated == TERMINATED_WITH_ERROR || terminated == TERMINATED_WITH_SUCCESS;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CAPACITY) return batchSize;
			if (key == Attr.BUFFERED) return batchSize - index; // TODO: shouldn't this
			// be index instead ? as it currently stands, the returned value represents
			// anticipated items left to fill buffer if completed before timeout
			if (key == Attr.RUN_ON) return timer;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(final T value) {
			int index;
			boolean flush;
			for(;;){
				index = this.index + 1;
				flush = index % batchSize == 0;
				if(INDEX.compareAndSet(this, index - 1, flush ? 0 : index)){
					break;
				}
			}

			if (index == 1) {
				try {
					timespanRegistration = timer.schedule(flushTask, timespan, unit);
				}
				catch (RejectedExecutionException ree) {
					Context ctx = actual.currentContext();
					onError(Operators.onRejectedExecution(ree, subscription, null, value, ctx));
					Operators.onDiscard(value, ctx);
					return;
				}
			}

			nextCallback(value);

			if (flush) {
				if (timespanRegistration != null) {
					timespanRegistration.dispose();
					timespanRegistration = null;
				}
				flushCallback(value);
			}
		}

		void checkedComplete() {
			try {
				flushCallback(null);
			}
			finally {
				actual.onComplete();
			}
		}

		/**
		 * @return has this {@link Subscriber} terminated with success ?
		 */
		final boolean isCompleted() {
			return terminated == TERMINATED_WITH_SUCCESS;
		}

		/**
		 * @return has this {@link Subscriber} terminated with an error ?
		 */
		final boolean isFailed() {
			return terminated == TERMINATED_WITH_ERROR;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				if (terminated != NOT_TERMINATED) {
					return;
				}
				if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
					requestMore(Long.MAX_VALUE);
				}
				else {
					long requestLimit = Operators.multiplyCap(requested, batchSize);
					if (requestLimit > outstanding) {
						requestMore(requestLimit - outstanding);
					}
				}
			}
		}

		final void requestMore(long n) {
			Subscription s = this.subscription;
			if (s != null) {
				Operators.addCap(OUTSTANDING, this, n);
				s.request(n);
			}
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		public void onComplete() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				timer.dispose();
				checkedComplete();
			}
		}

		@Override
		public void onError(Throwable throwable) {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
				timer.dispose();
				Context ctx = actual.currentContext();
				synchronized (this) {
					C v = values;
					if(v != null) {
						Operators.onDiscardMultiple(v, ctx);
						v.clear();
						values = null;
					}
				}
				actual.onError(throwable);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.subscription, s)) {
				this.subscription = s;
				doOnSubscribe();
				actual.onSubscribe(this);
			}
		}

		@Override
		public void cancel() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				timer.dispose();
				Subscription s = this.subscription;
				if (s != null) {
					this.subscription = null;
					s.cancel();
				}
				C v = values;
				if (v != null) {
					Operators.onDiscardMultiple(v, actual.currentContext());
					v.clear();
				}
			}
		}
	}
}


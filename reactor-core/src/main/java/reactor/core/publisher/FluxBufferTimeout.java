/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
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

		@Nullable
		final Logger                    logger;
		final CoreSubscriber<? super C> actual;
		final int batchSize;
		final int prefetch;
		final long timeSpan;
		final TimeUnit unit;
		final Scheduler.Worker timer;
		final Supplier<C> bufferSupplier;

		// tracks unsatisfied downstream demand (expressed in # of buffers)
		volatile long requested;
		@SuppressWarnings("rawtypes")
		private AtomicLongFieldUpdater<BufferTimeoutWithBackpressureSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "requested");

		// tracks undelivered values in the current buffer
		volatile int index;
		@SuppressWarnings("rawtypes")
		private AtomicIntegerFieldUpdater<BufferTimeoutWithBackpressureSubscriber> INDEX =
				AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "index");

		// tracks # of values requested from upstream but not delivered yet via this
		// .onNext(v)
		volatile long outstanding;
		@SuppressWarnings("rawtypes")
		private AtomicLongFieldUpdater<BufferTimeoutWithBackpressureSubscriber> OUTSTANDING =
				AtomicLongFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "outstanding");

		// indicates some thread is draining
		volatile int wip;
		@SuppressWarnings("rawtypes")
		private AtomicIntegerFieldUpdater<BufferTimeoutWithBackpressureSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "wip");

		private volatile int terminated = NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		private AtomicIntegerFieldUpdater<BufferTimeoutWithBackpressureSubscriber> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutWithBackpressureSubscriber.class, "terminated");

		final static int NOT_TERMINATED          = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR   = 2;
		final static int TERMINATED_WITH_CANCEL  = 3;

		@Nullable
		private Subscription subscription;

		private Queue<T> queue;

		@Nullable
		Throwable error;

		boolean completed;

		Disposable currentTimeoutTask;

		public BufferTimeoutWithBackpressureSubscriber(
				CoreSubscriber<? super C> actual,
				int batchSize,
				long timeSpan,
				TimeUnit unit,
				Scheduler.Worker timer,
				Supplier<C> bufferSupplier,
				@Nullable Logger logger) {
			this.actual = actual;
			this.batchSize = batchSize;
			this.timeSpan = timeSpan;
			this.unit = unit;
			this.timer = timer;
			this.bufferSupplier = bufferSupplier;
			this.logger = logger;
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
		public void onNext(T t) {
			if (logger != null) {
				trace(logger, "onNext: " + t);
			}
			// check if terminated (cancelled / error / completed) -> discard value if so

			// increment index
			// append to buffer
			// drain

			if (terminated == NOT_TERMINATED) {
				// assume no more deliveries than requested
				if (!queue.offer(t)) {
					Context ctx = currentContext();
					Throwable error = Operators.onOperatorError(this.subscription,
							Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
							t, actual.currentContext());
					this.error = error;
					if (!TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
						Operators.onErrorDropped(error, ctx);
						return;
					}
					Operators.onDiscard(t, ctx);
					drain();
					return;
				}

				boolean shouldDrain = false;
				for (;;) {
					int index = this.index;
					if (INDEX.compareAndSet(this, index, index + 1)) {
						if (index == 0) {
							try {
								if (logger != null) {
									trace(logger, "timerStart");
								}
								currentTimeoutTask = timer.schedule(this::bufferTimedOut,
										timeSpan,
										unit);
							} catch (RejectedExecutionException ree) {
								if (logger != null) {
									trace(logger, "Timer rejected for " + t);
								}
								Context ctx = actual.currentContext();
								Throwable error = Operators.onRejectedExecution(ree, subscription, null, t, ctx);
								this.error = error;
								if (!TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
									Operators.onDiscard(t, ctx);
									Operators.onErrorDropped(error, ctx);
									return;
								}
								if (logger != null) {
									trace(logger, "Discarding upon timer rejection" + t);
								}
								Operators.onDiscard(t, ctx);
								drain();
								return;
							}
						}
						if ((index + 1) % batchSize == 0) {
							shouldDrain = true;
						}
						break;
					}
				}
				if (shouldDrain) {
					if (currentTimeoutTask != null) {
						// TODO: it can happen that AFTER I dispose, the timeout
						//  anyway kicks during/after another onNext(), the buffer is
						//  delivered, and THEN drain is entered ->
						//  it would emit a buffer that is too small potentially.
						//  ALSO:
						//  It is also possible that here we deliver the buffer, but the
						//  timeout is happening for a new buffer!
						currentTimeoutTask.dispose();
					}
					this.index = 0;
					drain();
				}
			} else {
				if (logger != null) {
					trace(logger, "Discarding onNext: " + t);
				}
				Operators.onDiscard(t, currentContext());
			}
		}

		@Override
		public void onError(Throwable t) {
			// set error flag
			// set terminated as error

			// drain (WIP++ ?)

			if (currentTimeoutTask != null) {
				currentTimeoutTask.dispose();
			}
			timer.dispose();

			if (!TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}
			this.error = t; // wip in drain will publish the error
			drain();
		}

		@Override
		public void onComplete() {
			// set terminated as completed
			// drain
			if (currentTimeoutTask != null) {
				currentTimeoutTask.dispose();
			}
			timer.dispose();

			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				drain();
			}
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return this.actual;
		}

		@Override
		public void request(long n) {
			// add cap to currently requested
			// if previous requested was 0 -> drain first to deliver outdated values
			// if the cap increased, request more ?

			// drain

			if (Operators.validate(n)) {
				if (queue.isEmpty() && terminated != NOT_TERMINATED) {
					return;
				}

				if (Operators.addCap(REQUESTED, this, n) == 0) {
					// there was no demand before - try to fulfill the demand if there
					// are buffered values
					drain();
				}

				if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
					requestMore(Long.MAX_VALUE);
				} else {
					long requestLimit = prefetch;
					if (requestLimit > outstanding) {
						if (logger != null) {
							trace(logger, "requestMore: " + (requestLimit - outstanding) + ", outstanding: " + outstanding);
						}
						requestMore(requestLimit - outstanding);
					}
				}
			}
		}

		private void requestMore(long n) {
			Subscription s = this.subscription;
			if (s != null) {
				Operators.addCap(OUTSTANDING, this, n);
				s.request(n);
			}
		}

		@Override
		public void cancel() {
			// set terminated flag
			// cancel upstream subscription
			// dispose timer
			// drain for proper cleanup

			if (logger != null) {
				trace(logger, "cancel");
			}
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				if (this.subscription != null) {
					this.subscription.cancel();
				}
			}
			if (currentTimeoutTask != null) {
				currentTimeoutTask.dispose();
			}
			timer.dispose();
			drain();
		}

		void bufferTimedOut() {
			// called when buffer times out

			// reset index to 0
			// drain

			// TODO: try comparing against current reference and see if it was not
			//  cancelled -> to do this, replace Disposable timeoutTask with volatile
			//  and use CAS.
			if (logger != null) {
				trace(logger, "timerFire");
			}
			this.index = 0; // if currently being drained, it means the buffer is
			// delivered due to reaching the batchSize
			drain();
		}

		private void drain() {
			// entering this should be guarded by WIP getAndIncrement == 0
			// if we're here it means do a flush if there is downstream demand
			// regardless of queue size

			// loop:
			//   if terminated -> check error -> deliver; else complete downstream
			//   if cancelled

			if (WIP.getAndIncrement(this) == 0) {
				for (;;) {
					int wip = this.wip;
					if (logger != null) {
						trace(logger, "drain. wip: " + wip);
					}
					if (terminated == NOT_TERMINATED) {
						// is there demand?
						while (flushABuffer()) {
							// no-op
						}
						// make another spin if there's more work
					} else {
						if (completed) {
							// if queue is empty, the discard is ignored
							if (logger != null) {
								trace(logger, "Discarding entire queue of " + queue.size());
							}
							Operators.onDiscardQueueWithClear(queue, currentContext(),
									null);
							return;
						}
						// TODO: potentially the below can be executed twice?
						if (terminated == TERMINATED_WITH_CANCEL) {
							if (logger != null) {
								trace(logger, "Discarding entire queue of " + queue.size());
							}
							Operators.onDiscardQueueWithClear(queue, currentContext(),
									null);
							return;
						}
						while (flushABuffer()) {
							// no-op
						}
						if (queue.isEmpty()) {
							completed = true;
							if (this.error != null) {
								actual.onError(this.error);
							}
							else {
								actual.onComplete();
							}
						} else {
							if (logger != null) {
								trace(logger, "Queue not empty after termination");
							}
						}
					}
					if (WIP.compareAndSet(this, wip, 0)) {
						break;
					}
				}
			}
		}

		boolean flushABuffer() {
			long requested = this.requested;
			if (requested != 0) {
				T element;
				C buffer;

				element = queue.poll();
				if (element == null) {
					// there is demand, but queue is empty
					return false;
				}
				buffer = bufferSupplier.get();
				int i = 0;
				do {
					buffer.add(element);
				} while ((++i < batchSize) && ((element = queue.poll()) != null));

				if (requested != Long.MAX_VALUE) {
					requested = REQUESTED.decrementAndGet(this);
				}

				if (logger != null) {
					trace(logger, "flush: " + buffer + ", now requested: " + requested);
				}

				actual.onNext(buffer);

				if (requested != Long.MAX_VALUE) {
					if (logger != null) {
						trace(logger, "outstanding(" + outstanding + ") -= " + i);
					}
					long remaining = OUTSTANDING.addAndGet(this, -i);
					if (terminated == NOT_TERMINATED) {
						int replenishMark = prefetch >> 1; // TODO: create field limit instead
						if (remaining < replenishMark) {
							if (logger != null) {
								trace(logger, "replenish: " + (prefetch - remaining) + ", outstanding: " + outstanding);
							}
							requestMore(prefetch - remaining);
						}
					}

					if (requested <= 0) {
						return false;
					}
				}
				// continue to see if there's more
				return true;
			}
			return false;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return this.subscription;
			if (key == Attr.CANCELLED) return terminated == TERMINATED_WITH_CANCEL;
			if (key == Attr.TERMINATED) return terminated == TERMINATED_WITH_ERROR || terminated == TERMINATED_WITH_SUCCESS;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CAPACITY) return prefetch; // TODO: revise
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.RUN_ON) return timer;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
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


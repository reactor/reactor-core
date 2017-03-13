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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.scheduler.Scheduler;

/**
 * @author Stephane Maldini
 */
abstract class FluxBatch<T, V> extends FluxSource<T, V> {

	final int            batchSize;
	final long           timespan;
	final Scheduler timer;

	public FluxBatch(Publisher<T> source,
			int batchSize,
			long timespan,
			final Scheduler timer) {
		super(source);
		if (timespan <= 0) {
			throw new IllegalArgumentException("Timeout period must be strictly " + "positive");
		}
		if (batchSize <= 0) {
			throw new IllegalArgumentException("BatchSize period must be strictly " +
					"positive");
		}
		this.timer = Objects.requireNonNull(timer, "Timer");
		this.timespan = timespan;
		this.batchSize = batchSize;
	}

	final Subscriber<? super V> prepareSub(Subscriber<? super V> actual) {
		return Operators.serialize(actual);
	}

	static abstract class BatchAction<T, V> extends Operators.SubscriberAdapter<T, V> {

		@SuppressWarnings("ThrowableInstanceNeverThrown")
		static final Exception FAILED_SATE             =
				new RuntimeException("Failed Subscriber") {
					/** */
					private static final long serialVersionUID = 7503907754069414227L;

					@Override
					public synchronized Throwable fillInStackTrace() {
						return null;
					}
				};
		final static int NOT_TERMINATED          = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR   = 2;

		final static int TERMINATED_WITH_CANCEL  = 3;
		final boolean                    first;
		final int                        batchSize;
		final long                       timespan;
		final Scheduler.Worker           timer;
		final Runnable                   flushTask;

		volatile int                                    terminated =
				NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		static final     AtomicIntegerFieldUpdater<BatchAction> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BatchAction.class, "terminated");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BatchAction> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BatchAction.class, "requested");

		volatile int index = 0;

		static final     AtomicIntegerFieldUpdater<BatchAction> INDEX =
				AtomicIntegerFieldUpdater.newUpdater(BatchAction.class, "index");

		volatile Cancellation timespanRegistration;

		public BatchAction(Subscriber<? super V> actual,
				int batchSize,
				boolean first,
				long timespan,
				final Scheduler.Worker timer) {

			super(actual);

			this.timespan = timespan;
			this.timer = timer;
			this.flushTask = () -> {
				if (!isTerminated()) {
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

			this.first = first;
			this.batchSize = batchSize;
		}

		void doRequested(long before, long n) {
			if (isTerminated()) {
				return;
			}
			if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
				requestMore(Long.MAX_VALUE);
			}
			else {
				requestMore(Operators.multiplyCap(n, batchSize));
			}
		}

		void nextCallback(T event) {
		}

		void flushCallback(T event) {
		}

		void firstCallback(T event) {
		}

		@Override
		protected void doNext(final T value) {
			int index;
			for(;;){
				index = this.index + 1;
				if(INDEX.compareAndSet(this, index - 1, index)){
					break;
				}
			}

			if (index == 1) {
				timespanRegistration =
						timer.schedule(flushTask, timespan, TimeUnit.MILLISECONDS);
				if (timespanRegistration == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, value);
				}
				if (first) {
					firstCallback(value);
				}
			}

			nextCallback(value);

			if (this.index % batchSize == 0) {
				this.index = 0;
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
				subscriber.onComplete();
			}
		}

		@Override
		public boolean isTerminated() {
			return terminated != NOT_TERMINATED;
		}

		/**
		 * @return has this {@link Subscriber} terminated with success ?
		 */
		public final boolean isCompleted() {
			return terminated == TERMINATED_WITH_SUCCESS;
		}

		/**
		 * @return has this {@link Subscriber} terminated with an error ?
		 */
		public final boolean isFailed() {
			return terminated == TERMINATED_WITH_ERROR;
		}

		/**
		 * @return has this {@link Subscriber} been cancelled
		 */
		@Override
		public final boolean isCancelled() {
			return terminated == TERMINATED_WITH_CANCEL;
		}

		@Override
		public final long requestedFromDownstream() {
			return requested;
		}

		@Override
		protected void doRequest(long n) {
			doRequested(Operators.getAndAddCap(REQUESTED, this, n), n);
		}

		final void requestMore(long n) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
			}
		}

		@Override
		protected void doComplete() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				checkedComplete();
				doTerminate();
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
				checkedError(throwable);
				doTerminate();
			}
		}

		void checkedError(Throwable throwable) {
			subscriber.onError(throwable);
		}

		@Override
		protected void doCancel() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				checkedCancel();
				doTerminate();
			}
		}

		void checkedCancel() {
			super.doCancel();
		}

		void doTerminate() {
			timer.dispose();
		}

		@Override
		public Throwable getError() {
			return isFailed() ? FAILED_SATE : null;
		}


		@Override
		public String toString() {
			return super.toString() + "{" + (timer != null ?
					"timed - " + timespan + " ms" : "") + " batchSize=" +
					index + "/" +
					batchSize + " [" + (int) ((((float) index) / ((float) batchSize)) * 100) + "%]";
		}

	}

}

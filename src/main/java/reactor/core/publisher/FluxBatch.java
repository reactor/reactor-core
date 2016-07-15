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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionHelper;

/**
 * @author Stephane Maldini
 */
abstract class FluxBatch<T, V> extends FluxSource<T, V> {

	final boolean        next;
	final boolean        flush;
	final boolean        first;
	final int            batchSize;
	final long           timespan;
	final TimedScheduler timer;

	public FluxBatch(Publisher<T> source,
			int batchSize,
			boolean next,
			boolean first,
			boolean flush,
			long timespan, final TimedScheduler timer) {
		super(source);
		if (timespan > 0) {
			this.timespan = timespan;
			this.timer = timer;
		}
		else {
			this.timespan = -1L;
			this.timer = null;
		}

		this.first = first;
		this.flush = flush;
		this.next = next;
		this.batchSize = batchSize;
	}

	final Subscriber<? super V> prepareSub(Subscriber<? super V> actual) {
		if (timer != null) {
			return Subscribers.serialize(actual);
		}
		else {
			return actual;
		}
	}

	static abstract class BatchAction<T, V> extends SubscriberBarrier<T, V> {

		final static int NOT_TERMINATED = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR = 2;
		final static int TERMINATED_WITH_CANCEL = 3;

		private volatile       int                                             terminated = NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BatchAction> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BatchAction.class, "terminated");

		private volatile       long                                         requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BatchAction> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BatchAction.class, "requested");


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
		public final boolean isCancelled() {
			return terminated == TERMINATED_WITH_CANCEL;
		}

		@Override
		public final long requestedFromDownstream() {
			return requested;
		}

		@Override
		protected void doRequest(long n) {
			doRequested(SubscriptionHelper.getAndAddCap(REQUESTED, this, n), n);
		}

		final void requestMore(long n){
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

		void checkedError(Throwable throwable){
			subscriber.onError(throwable);
		}

		@Override
		protected void doCancel() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				checkedCancel();
				doTerminate();
			}
		}

		void checkedCancel(){
			super.doCancel();
		}

		void doTerminate(){
			//TBD
		}

		@Override
		public Throwable getError() {
			return isFailed() ? FAILED_SATE : null;
		}

		private static final Exception FAILED_SATE = new RuntimeException("Failed Subscriber"){
			/** */
            private static final long serialVersionUID = 7503907754069414227L;

            @Override
			public synchronized Throwable fillInStackTrace() {
				return null;
			}
		};

		final boolean        next;
		final boolean        flush;
		final boolean        first;
		final int            batchSize;
		final long           timespan;
		final TimedScheduler timer;
		final Runnable       flushTask;

		private volatile int index = 0;
		private Cancellation timespanRegistration;

		public BatchAction(Subscriber<? super V> actual,
				int batchSize,
				boolean next,
				boolean first,
				boolean flush,
				long timespan, final TimedScheduler timer) {

			super(actual);

			if (timespan > 0 && timer != null) {
				this.timespan = timespan;
				this.timer = timer;
				this.flushTask = () -> {
						if (!isTerminated()) {
							synchronized (timer) {
								if (index == 0) {
									return;
								}
								else {
									index = 0;
								}
							}
							flushCallback(null);
						}
				};
			}
			else {
				this.timespan = -1L;
				this.timer = null;
				this.flushTask = null;
			}
			this.first = first;
			this.flush = flush;
			this.next = next;
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
				requestMore(SubscriptionHelper.multiplyCap(n, batchSize));
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
			final int index;
			if (timer != null) {
				synchronized (timer) {
					index = ++this.index;
				}
			}
			else {
				index = ++this.index;
			}

			if (index == 1) {
				if (timer != null) {
					timespanRegistration = timer.schedule(flushTask, timespan, TimeUnit.MILLISECONDS);
				}
				if (first) {
					firstCallback(value);
				}
			}

			if (next) {
				nextCallback(value);
			}

			if (index % batchSize == 0) {
				if (timer != null && timespanRegistration != null) {
					timespanRegistration.dispose();
					timespanRegistration = null;
				}
				if (timer != null) {
					synchronized (timer) {
						this.index = 0;
					}
				}
				else {
					this.index = 0;
				}
				if (flush) {
					flushCallback(value);
				}
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
		public String toString() {
			return super.toString() + "{" + (timer != null ? "timed - " + timespan + " ms" : "") + " batchSize=" +
					index + "/" +
					batchSize + " [" + (int) ((((float) index) / ((float) batchSize)) * 100) + "%]";
		}

	}

}

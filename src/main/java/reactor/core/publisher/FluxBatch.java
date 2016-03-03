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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.state.Introspectable;
import reactor.core.state.Pausable;
import reactor.core.state.Requestable;

import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.timer.Timer;
import reactor.core.util.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.5
 */
abstract class FluxBatch<T, V> extends FluxSource<T, V> {

	protected final boolean  next;
	protected final boolean  flush;
	protected final boolean  first;
	protected final int      batchSize;
	protected final long     timespan;
	protected final Timer    timer;

	public FluxBatch(Publisher<T> source, int batchSize, boolean next, boolean first, boolean flush) {
		this(source, batchSize, next, first, flush, -1L, null);
	}

	public FluxBatch(Publisher<T> source,
			int batchSize,
			boolean next,
			boolean first,
			boolean flush,
			long timespan,
			final Timer timer) {
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

	protected final Subscriber<? super V> prepareSub(Subscriber<? super V> actual) {
		if (timer != null) {
			return SerializedSubscriber.create(actual);
		}
		else {
			return actual;
		}
	}

	static protected abstract class BatchAction<T, V> extends SubscriberBarrier<T, V>
			implements Requestable, Introspectable {

		protected final static int NOT_TERMINATED = 0;
		protected final static int TERMINATED_WITH_SUCCESS = 1;
		protected final static int TERMINATED_WITH_ERROR = 2;
		protected final static int TERMINATED_WITH_CANCEL = 3;

		@SuppressWarnings("unused")
		private volatile       int                                             terminated = NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		protected static final AtomicIntegerFieldUpdater<BatchAction> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BatchAction.class, "terminated");

		@SuppressWarnings("unused")
		private volatile       long                                         requested = 0L;
		@SuppressWarnings("rawtypes")
		protected static final AtomicLongFieldUpdater<BatchAction> REQUESTED =
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
			doRequested(BackpressureUtils.getAndAdd(REQUESTED, this, n), n);
		}

		protected final void requestMore(long n){
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

		protected void checkedError(Throwable throwable){
			subscriber.onError(throwable);
		}

		@Override
		protected void doCancel() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				checkedCancel();
				doTerminate();
			}
		}

		protected void checkedCancel(){
			super.doCancel();
		}

		protected void doTerminate(){
			//TBD
		}

		@Override
		public Throwable getError() {
			return isFailed() ? FAILED_SATE : null;
		}

		private static final Exception FAILED_SATE = new RuntimeException("Failed Subscriber"){
			@Override
			public synchronized Throwable fillInStackTrace() {
				return null;
			}
		};

		protected final boolean        next;
		protected final boolean        flush;
		protected final boolean        first;
		protected final int            batchSize;
		protected final long           timespan;
		protected final Timer          timer;
		protected final Consumer<Long> flushTask;

		private volatile int index = 0;
		private Pausable timespanRegistration;

		public BatchAction(Subscriber<? super V> actual,
				int batchSize,
				boolean next,
				boolean first,
				boolean flush,
				long timespan,
				final Timer timer) {

			super(actual);

			if (timespan > 0 && timer != null) {
				this.timespan = timespan;
				this.timer = timer;
				this.flushTask = new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {
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

		protected void doRequested(long before, long n) {
			if (isTerminated()) {
				return;
			}
			if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
				requestMore(Long.MAX_VALUE);
			}
			else {
				requestMore(BackpressureUtils.multiplyCap(n, batchSize));
			}
		}

		protected void nextCallback(T event) {
		}

		protected void flushCallback(T event) {
		}

		protected void firstCallback(T event) {
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
					timespanRegistration = timer.submit(flushTask, timespan);
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
					timespanRegistration.cancel();
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

		protected void checkedComplete() {
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

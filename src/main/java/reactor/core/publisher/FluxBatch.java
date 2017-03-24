/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

	FluxBatch(Flux<T> source,
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

	static abstract class BatchSubscriber<T, V> implements InnerOperator<T, V> {

		final Subscriber<? super V> actual;

		final static int NOT_TERMINATED          = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR   = 2;

		final static int TERMINATED_WITH_CANCEL  = 3;
		final boolean                    first;
		final int                        batchSize;
		final long                       timespan;
		final Scheduler.Worker           timer;
		final Runnable                   flushTask;

		protected Subscription subscription;

		volatile int                                                terminated =
				NOT_TERMINATED;
		@SuppressWarnings("rawtypes")
		static final     AtomicIntegerFieldUpdater<BatchSubscriber> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BatchSubscriber.class, "terminated");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BatchSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BatchSubscriber.class, "requested");

		volatile int index = 0;

		static final     AtomicIntegerFieldUpdater<BatchSubscriber> INDEX =
				AtomicIntegerFieldUpdater.newUpdater(BatchSubscriber.class, "index");

		volatile Cancellation timespanRegistration;

		BatchSubscriber(Subscriber<? super V> actual,
				int batchSize,
				boolean first,
				long timespan,
				final Scheduler.Worker timer) {

			this.actual = actual;
			this.timespan = timespan;
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

			this.first = first;
			this.batchSize = batchSize;
		}


		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return subscription;
				case CANCELLED:
					return terminated == TERMINATED_WITH_CANCEL;
				case TERMINATED:
					return terminated == TERMINATED_WITH_ERROR || terminated == TERMINATED_WITH_SUCCESS;
				case REQUESTED_FROM_DOWNSTREAM:
					return requested;
				case CAPACITY:
					return batchSize;
				case BUFFERED:
					return batchSize - index;
			}
			return InnerOperator.super.scan(key);
		}

		void nextCallback(T event) {
		}

		void flushCallback(T event) {
		}

		void firstCallback(T event) {
		}

		@Override
		public void onNext(final T value) {
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
				if (terminated != NOT_TERMINATED) {
					return;
				}
				if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
					requestMore(Long.MAX_VALUE);
				}
				else {
					requestMore(Operators.multiplyCap(n, batchSize));
				}
			}
		}

		final void requestMore(long n) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
			}
		}

		@Override
		public Subscriber<? super V> actual() {
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
				checkedError(throwable);
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

		abstract void doOnSubscribe();

		void checkedError(Throwable throwable) {
			actual.onError(throwable);
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
			}
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

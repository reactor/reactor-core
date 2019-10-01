/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.Collection;
import java.util.Objects;
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
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class FluxBufferTimeout<T, C extends Collection<? super T>> extends InternalFluxOperator<T, C> {

	final int            batchSize;
	final Supplier<C>    bufferSupplier;
	final Scheduler      timer;
	final long           timespan;

	FluxBufferTimeout(Flux<T> source,
			int maxSize,
			long timespan,
			Scheduler timer,
			Supplier<C> bufferSupplier) {
		super(source);
		if (timespan <= 0) {
			throw new IllegalArgumentException("Timeout period must be strictly positive");
		}
		if (maxSize <= 0) {
			throw new IllegalArgumentException("maxSize must be strictly positive");
		}
		this.timer = Objects.requireNonNull(timer, "Timer");
		this.timespan = timespan;
		this.batchSize = maxSize;
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		return new BufferTimeoutSubscriber<>(
				Operators.serialize(actual),
				batchSize,
				timespan,
				timer.createWorker(),
				bufferSupplier
		);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timer;

		return super.scanUnsafe(key);
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
				Scheduler.Worker timer,
				Supplier<C> bufferSupplier) {
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

			this.batchSize = maxSize;
			this.bufferSupplier = bufferSupplier;
		}

		protected void doOnSubscribe() {
			values = bufferSupplier.get();
		}

		void nextCallback(T value) {
			synchronized (this) {
				C v = values;
				if(v == null) {
					v = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
					values = v;
				}
				v.add(value);

				long next;
				long o = outstanding;
				if (o != 0) {
					for (; ; ) {
						next = o - 1;
						if (OUTSTANDING.compareAndSet(this, o, next)) {
							break;
						}
						o = outstanding;
						if (o <= 0L) {
							break;
						}
					}
				}
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
			if (key == Attr.BUFFERED) return batchSize - index;
			if (key == Attr.RUN_ON) return timer;

			return InnerOperator.super.scanUnsafe(key);
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
				try {
					timespanRegistration = timer.schedule(flushTask, timespan, TimeUnit.MILLISECONDS);
				}
				catch (RejectedExecutionException ree) {
					Context ctx = actual.currentContext();
					onError(Operators.onRejectedExecution(ree, subscription, null, value, ctx));
					Operators.onDiscard(value, ctx);
					return;
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
				Operators.addCap(REQUESTED, this, n);
				if (terminated != NOT_TERMINATED) {
					return;
				}
				if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
					requestMore(Long.MAX_VALUE);
				}
				else {
					long requestLimit = Operators.multiplyCap(requested, batchSize);
					requestMore(requestLimit - outstanding);
				}
			}
		}

		final void requestMore(long n) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
				Operators.addCap(OUTSTANDING, this, n);
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


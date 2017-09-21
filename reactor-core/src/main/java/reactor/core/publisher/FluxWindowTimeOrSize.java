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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * WindowTimeoutSubscriber is forwarding events on a steam until {@code maxSize} is reached,
 * after that streams collected events further, complete it and create a fresh new fluxion.
 * @author Stephane Maldini
 */
final class FluxWindowTimeOrSize<T> extends FluxOperator<T, Flux<T>> {

	final int            batchSize;
	final long           timespan;
	final Scheduler      timer;

	FluxWindowTimeOrSize(Flux<T> source, int maxSize, long timespan, Scheduler timer) {
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
	}

	@Override
	public void subscribe(CoreSubscriber<? super Flux<T>> actual) {
		source.subscribe(new WindowTimeoutSubscriber<>(Operators.serialize(actual),
				batchSize,
				timespan,
				timer));
	}

	final static class Window<T> extends Flux<T> implements InnerOperator<T, T> {

		final UnicastProcessor<T> processor;
		final Scheduler           timer;

		int count = 0;

		Window(Scheduler timer) {
			this.processor = UnicastProcessor.create();
			this.timer = timer;
		}

		@Override
		public void onSubscribe(Subscription s) {
		}

		@Override
		public void onNext(T t) {
			count++;
			processor.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			processor.onError(t);
		}

		@Override
		public void onComplete() {
			processor.onComplete();
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> actual) {
			processor.subscribe(actual);
		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return processor;
		}
	}

	final static class WindowTimeoutSubscriber<T> implements InnerOperator<T, Flux<T>> {

		final static int NOT_TERMINATED          = 0;
		final static int TERMINATED_WITH_SUCCESS = 1;
		final static int TERMINATED_WITH_ERROR   = 2;
		final static int TERMINATED_WITH_CANCEL  = 3;

		final CoreSubscriber<? super Flux<T>> actual;
		final int                             batchSize;
		final Runnable                        flushTask;
		final Scheduler.Worker                timer;
		final Scheduler                       timerScheduler;
		final long                            timespan;

		Window<T>    currentWindow;
		Subscription subscription;

		volatile Disposable timespanRegistration;

		volatile int  cancelled;
		volatile int  index;
		volatile int  initialWindowEmitted;
		volatile long requested;
		volatile int  terminated = NOT_TERMINATED;
		volatile int  windowCount;

		static final AtomicIntegerFieldUpdater<WindowTimeoutSubscriber>
				CANCELLED = AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "cancelled");

		static final AtomicIntegerFieldUpdater<WindowTimeoutSubscriber>
				INDEX = AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "index");

		static final AtomicIntegerFieldUpdater<WindowTimeoutSubscriber>
				INITIAL_WINDOW_EMITTED = AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "initialWindowEmitted");

		static final AtomicLongFieldUpdater<WindowTimeoutSubscriber>
				REQUESTED = AtomicLongFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "requested");

		static final     AtomicIntegerFieldUpdater<WindowTimeoutSubscriber>
				TERMINATED = AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "terminated");

		static final AtomicIntegerFieldUpdater<WindowTimeoutSubscriber>
				WINDOW_COUNT = AtomicIntegerFieldUpdater.newUpdater(WindowTimeoutSubscriber.class, "windowCount");

		WindowTimeoutSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int maxSize,
				long timespan,
				Scheduler timer) {
			this.actual = actual;
			this.timespan = timespan;
			this.timerScheduler = timer;
			this.timer = timer.createWorker();
			this.flushTask = () -> {
				if (terminated == NOT_TERMINATED) {
					int index;
					for(;;){
						index = this.index;
						if(INDEX.compareAndSet(this, index, 0)){
							break;
						}
					}
					windowCloseByTimeout(); //this restarts a timer
				}
			};

			this.batchSize = maxSize;
			WINDOW_COUNT.lazySet(this, 1);
		}

		//this is necessary so that the case where timer is rejected from the beginning is handled correctly
		void subscribeAndCreateWindow() {
			try {
				timespanRegistration = timer.schedule(flushTask, timespan, TimeUnit.MILLISECONDS);
				WINDOW_COUNT.getAndIncrement(this);
				currentWindow = new Window<>(timerScheduler);
				actual.onSubscribe(this);
				//hold on emitting the window until either the first close by timeout
				//or the first emission, which will follow the subscribe
			}
			catch (RejectedExecutionException ree) {
				RuntimeException error = Operators.onRejectedExecution(ree, subscription, null, null,
						actual.currentContext());
				Operators.error(actual, error);
			}
		}

		void windowCreateAndEmit() {
			if (timerStart()) {
				WINDOW_COUNT.getAndIncrement(this);
				Window<T> _currentWindow = new Window<>(timerScheduler);
				currentWindow = _currentWindow;
				actual.onNext(_currentWindow);
			}
		}

		void windowCloseByTimeout() {
			if (currentWindow != null) {
				if (INITIAL_WINDOW_EMITTED.compareAndSet(this, 0, 1)) {
					actual.onNext(currentWindow);
				}
				currentWindow.onComplete();
				currentWindow = null;
				dispose();
			}
			windowCreateAndEmit();
		}

		void windowCloseBySize() {
			if (currentWindow != null) {
				currentWindow.onComplete();
				currentWindow = null;
				dispose();
			}
			windowCreateAndEmit();
		}

		boolean timerStart() {
			try {
				timespanRegistration = timer.schedule(flushTask, timespan, TimeUnit.MILLISECONDS);
				return true;
			}
			catch (RejectedExecutionException ree) {
				onError(Operators.onRejectedExecution(ree, actual.currentContext()));
				return false;
			}
		}

		void timerCancel() {
			if (timespanRegistration != null) {
				timespanRegistration.dispose();
				timespanRegistration = null;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.subscription, s)) {
				this.subscription = s;
				subscribeAndCreateWindow();
			}
		}

		@Override
		public void onNext(final T value) {
			if (currentWindow != null
				&& INITIAL_WINDOW_EMITTED.compareAndSet(this, 0, 1)) {
				actual.onNext(currentWindow);
			}

			int index;
			for(;;){
				index = this.index + 1;
				if(INDEX.compareAndSet(this, index - 1, index)){
					break;
				}
			}

			if (currentWindow != null) { //not encapsulated in a method to avoid stack too deep
				currentWindow.onNext(value);
			}

			if (index % batchSize == 0) {
				this.index = 0;
				timerCancel();
				windowCloseBySize();
			}
		}

		@Override
		public void onComplete() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				timerCancel();
				timer.dispose();
				if (currentWindow != null) {
					currentWindow.onComplete();
					currentWindow = null;
					dispose();
				}
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable throwable) {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
				timerCancel();
				timer.dispose();
				if (currentWindow != null) {
					currentWindow.onError(throwable);
					currentWindow = null;
					dispose();
				}
				actual.onError(throwable);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.TERMINATED) return terminated == TERMINATED_WITH_ERROR || terminated == TERMINATED_WITH_SUCCESS;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CAPACITY) return batchSize;
			if (key == Attr.BUFFERED) return batchSize - index;

			return InnerOperator.super.scanUnsafe(key);
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
		public CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		void doCancel() {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
				timer.dispose();
				Subscription s = this.subscription;
				if (s != null) {
					this.subscription = null;
					s.cancel();
				}
			}
		}

		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				if (cancelled == 1)
					doCancel();
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

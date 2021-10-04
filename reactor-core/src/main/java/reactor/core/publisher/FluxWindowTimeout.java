/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
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
import reactor.core.scheduler.Schedulers;
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
	final boolean fairBackpressure;

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

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowTimeoutWithBackpressureSubscriber> STATE =
				AtomicLongFieldUpdater.newUpdater(WindowTimeoutWithBackpressureSubscriber.class, "state");

		static final long HAS_ACTIVE_WINDOW = 0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		long producerIndex;

		Subscription s;

		volatile InnerWindow<T> window;

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

			this.window.sendNext(t);
		}

		@Override
		public void onError(Throwable t) {
			this.actual.onError(t);
			final InnerWindow<T> window = this.window;
			if (window != null) {
				window.sendError(t);
			}
		}

		@Override
		public void onComplete() {
			final InnerWindow<T> window = this.window;
			if (window != null) {
				window.sendComplete();
			}
			this.actual.onComplete();
		}

		@Override
		public void request(long n) {
			for (;;) {
				final long state = this.state;
				final long requested = state & Long.MAX_VALUE;

				if (requested == Long.MAX_VALUE) {
					return;
				}

				final InnerWindow<T> previousActiveWindow = this.window;
				final long hasActiveWindowFlag = state & HAS_ACTIVE_WINDOW;
				final long nextRequested = Operators.addCap(requested, hasActiveWindowFlag != HAS_ACTIVE_WINDOW ? n - 1 : n);

				if (STATE.compareAndSet(this, state, HAS_ACTIVE_WINDOW | nextRequested)) {
					if (requested == 0 && hasActiveWindowFlag != HAS_ACTIVE_WINDOW) {
						final InnerWindow<T> nextWindow = new InnerWindow<>(this.maxSize, this.s, this);
						this.window = nextWindow;

						this.actual.onNext(nextWindow);

						if (previousActiveWindow != null) {
							previousActiveWindow.sendComplete();
							this.s.request(previousActiveWindow.received);
						} else {
							this.s.request(this.maxSize);
						}
					}
					return;
				}
			}
		}

		@Override
		public void cancel() {
			this.s.cancel();
			this.window.cancelFromParent();
		}

		boolean tryCreateNextWindow(InnerWindow<T> currentWindow) {
			for (;;) {
				final long state = this.state;
				final long requested = state & Long.MAX_VALUE;

				if (requested > 0) {
					if (requested != Long.MAX_VALUE && !STATE.compareAndSet(this, state, (requested - 1) | HAS_ACTIVE_WINDOW)) {
						continue;
					}

					final InnerWindow<T> nextWindow = new InnerWindow<>(this.maxSize, this.s, this);
					this.window = nextWindow;
					this.actual.onNext(nextWindow);
					this.s.request(currentWindow.received);

					return true;
				}

				if (STATE.compareAndSet(this, state, 0)) {
					return false;
				}
			}
		}

		Disposable schedule(Runnable runnable) {
			return worker.schedule(runnable, timespan, unit);
		}
	}

	static final class InnerWindow<T> extends Flux<T>
			implements InnerProducer<T>, Fuseable.QueueSubscription<T>, Runnable {

		final WindowTimeoutWithBackpressureSubscriber<T> parent;
		final int                                        max;
		final Subscription                               upstream;
		final Queue<T>                                   queue;
		final Disposable                                 timer;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerWindow> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerWindow.class, "requested");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerWindow> STATE =
				AtomicLongFieldUpdater.newUpdater(InnerWindow.class, "state");

		CoreSubscriber<? super T> actual;

		boolean   done;
		Throwable error;

		int mode;

		int received = 0;

		InnerWindow(
				int max,
				Subscription upstream,
				WindowTimeoutWithBackpressureSubscriber<T> parent) {
			this.max = max;
			this.upstream = upstream;
			this.parent = parent;
			this.queue = Queues.<T>get(max).get();
			this.timer = parent.schedule(this);
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

			markSubscriberSet(this);
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

			if (hasWorkInProgress(previousState) || isCancelled(previousState)) {
				return;
			}

			if (hasValues(previousState)) {
				drain((previousState | HAS_SUBSCRIBER_SET_STATE | HAS_REQUEST_STATE) + 1);
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

		void cancelFromParent() {
			for (;;) {
				final long state = this.state;

				if (isCancelled(state) || isTerminated(state) || isTimeout(state) || isFinalized(state)) {
					return;
				}

				if (STATE.compareAndSet(this, state, state | CANCELLED_PARENT_STATE)) {
					return;
				}
			}
		}

		void sendNext(T t) {
			int received = this.received + 1 ;
			this.received = received;

			this.queue.offer(t);

			final long previousState;
			final long nextState;
			if (received == this.max) {
				this.done = true;
				previousState = markHasValuesAndTerminated(this);
				nextState = (previousState | TERMINATED_STATE | HAS_VALUES_STATE) + 1;

				if (!isTimeout(previousState)) {
					this.timer.dispose();
					this.parent.tryCreateNextWindow(this);
				}
			} else {
				previousState = markHasValues(this);
				nextState = (previousState | HAS_VALUES_STATE) + 1;
			}

			if (isFinalized(previousState)) {
				clearQueue();
				return;
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onNext(t);
					return;
				}

				if (hasWorkInProgress(previousState)) {
					return;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return;
				}

				if (hasRequest(previousState)) {
					drain(nextState);
				}
			}
		}

		void drain(long expectedState) {
			final Queue<T> q = this.queue;
			final CoreSubscriber<? super T> a = this.actual;

			long r = this.requested;

			for (; ; ) {
				long e = 0;
				while (e < r) {
					final boolean done = this.done;
					final T v = q.poll();
					final boolean empty = v == null;

					if (checkTerminated(done, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
				}

				final boolean done = this.done;
				final boolean empty = q.isEmpty();
				if (checkTerminated(done, empty, a, null)) {
					return;
				}

				if (r != Long.MAX_VALUE) {
					r = REQUESTED.addAndGet(this, -e);
				}

				expectedState = markWorkDone(this, expectedState, r > 0, empty);
				if (isCancelled(expectedState) || !hasWorkInProgress(expectedState)) {
					return;
				}
			}
		}

		boolean checkTerminated(
				boolean done,
				boolean isEmpty,
				CoreSubscriber<? super T> actual,
				@Nullable T value) {
			if (isCancelled(this.state)) {
				if (value != null) {
					Operators.onDiscard(value, actual.currentContext());
				}
				clearAndFinalize();
				return true;
			}

			if (isEmpty && done) {
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

		void sendError(Throwable t) {
			if (this.done) {
				return;
			}

			this.error = t;
			this.done = true;

			final long previousState = markTerminated(this);
			if (isFinalized(previousState) || isTerminated(previousState)) {
				return;
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onError(t);
				}

				if (hasWorkInProgress(previousState)) {
					return;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return;
				}

				drain((previousState | TERMINATED_STATE) + 1);
			}
		}

		void sendComplete() {
			if (this.done) {
				return;
			}

			this.done = true;

			final long previousState = markTerminated(this);
			if (isFinalized(previousState) || isTerminated(previousState)) {
				return;
			}

			if (hasSubscriberSet(previousState)) {
				if (this.mode == ASYNC) {
					this.actual.onComplete();
				}

				if (hasWorkInProgress(previousState)) {
					return;
				}

				if (isCancelled(previousState)) {
					clearAndFinalize();
					return;
				}

				drain((previousState | TERMINATED_STATE) + 1);
			}
		}

		@Override
		public void run() {
			if (this.done) {
				return;
			}

			long previousState = markTimeout(this);
			if (isTerminated(previousState)) {
				return;
			}

			if (!isCancelledFromParent(previousState) && !this.parent.tryCreateNextWindow(this)) {
				return;
			}

			sendComplete();
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

				if (STATE.compareAndSet(this,
						state,
						(state | FINALIZED_STATE) & ~WORK_IN_PROGRESS_MAX)) {
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
		static final long CANCELLED_STATE          =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long TIMEOUT_STATE            =
				0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long CANCELLED_PARENT_STATE   =
				0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_VALUES_STATE         =
				0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_REQUEST_STATE        =
				0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_STATE     =
				0b0000_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long HAS_SUBSCRIBER_SET_STATE =
				0b0000_0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long WORK_IN_PROGRESS_MAX     =
				0b0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_1111_1111_1111L;

		static <T> long markTerminated(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isTerminated(state) || isCancelled(state)) {
					return state;
				}

				final long nextState = state | TERMINATED_STATE | (
						(hasSubscriberSet(state) || hasWorkInProgress(state)) ?
								incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_STATE) == TERMINATED_STATE;
		}

		static <T> long markTimeout(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isTerminated(state)) {
					return state;
				}

				final long nextState = state | TIMEOUT_STATE;
				if (STATE.compareAndSet(instance, state, nextState)) {
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

				if (isCancelled(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance,
						state,
						state | CANCELLED_STATE | incrementWork(state))) {
					return state;
				}
			}
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_STATE) == CANCELLED_STATE;
		}

		static boolean isCancelledFromParent(long state) {
			return (state & CANCELLED_PARENT_STATE) == CANCELLED_PARENT_STATE;
		}

		static <T> long markHasValues(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long nextState;
				if (hasSubscriberSet(state)) {
					if (instance.mode == ASYNC) {
						return state;
					}

					nextState = state | HAS_VALUES_STATE | (
							(hasRequest(state) || hasWorkInProgress(state)) ?
									incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);
				}
				else {
					nextState = state | HAS_VALUES_STATE | (hasWorkInProgress(state) ?
							incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static <T> long markHasValuesAndTerminated(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long nextState;
				if (hasSubscriberSet(state)) {
					if (instance.mode == ASYNC) {
						return state;
					}

					nextState = state | HAS_VALUES_STATE | TERMINATED_STATE | (
							(hasRequest(state) || hasWorkInProgress(state)) ?
									incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);
				}
				else {
					nextState = state | HAS_VALUES_STATE | TERMINATED_STATE | (hasWorkInProgress(state) ?
							incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);
				}

				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long incrementWork(long currentWork) {
			return currentWork == WORK_IN_PROGRESS_MAX ? WORK_IN_PROGRESS_MAX :
					currentWork + 1;
		}

		static boolean hasValues(long state) {
			return (state & HAS_VALUES_STATE) == HAS_VALUES_STATE;
		}

		static <T> long markHasRequest(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final long nextState = state | HAS_REQUEST_STATE | (
						(hasValues(state) || hasWorkInProgress(state)) ?
								incrementWork((state & WORK_IN_PROGRESS_MAX)) : 0);

				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean hasRequest(long state) {
			return (state & HAS_REQUEST_STATE) == HAS_REQUEST_STATE;
		}

		static <T> long markSubscribedOnce(InnerWindow<T> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (hasSubscribedOnce(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | HAS_SUBSCRIBER_STATE)) {
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

				if (hasSubscriberSet(state) || isCancelled(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance,
						state,
						state | HAS_SUBSCRIBER_SET_STATE)) {
					return state;
				}
			}
		}

		static boolean hasSubscriberSet(long state) {
			return (state & HAS_SUBSCRIBER_SET_STATE) == HAS_SUBSCRIBER_SET_STATE;
		}

		static <T> long markWorkDone(InnerWindow<T> instance,
				long expectedState,
				boolean hasRequest,
				boolean hasValues) {
			for (; ; ) {
				final long state = instance.state;

				if (expectedState != state) {
					return state;
				}

				final long nextState =
						(state & ~WORK_IN_PROGRESS_MAX) | HAS_SUBSCRIBER_SET_STATE | (hasRequest ? HAS_REQUEST_STATE : 0) | (hasValues ? HAS_VALUES_STATE : 0);
				if (STATE.compareAndSet(instance, state, nextState)) {
					return nextState;
				}
			}
		}

		static boolean hasWorkInProgress(long state) {
			return (state & WORK_IN_PROGRESS_MAX) > 0;
		}

		static boolean isFinalized(long state) {
			return (state & FINALIZED_STATE) == FINALIZED_STATE;
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

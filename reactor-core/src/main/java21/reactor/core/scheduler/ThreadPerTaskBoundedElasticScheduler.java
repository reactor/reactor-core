/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import static reactor.core.scheduler.ThreadPerTaskBoundedElasticScheduler.BoundedServices.CREATING;
import static reactor.core.scheduler.ThreadPerTaskBoundedElasticScheduler.BoundedServices.SHUTDOWN;
import static reactor.core.scheduler.ThreadPerTaskBoundedElasticScheduler.BoundedServices;
import static reactor.core.scheduler.Schedulers.onSchedule;

final class ThreadPerTaskBoundedElasticScheduler
		implements Scheduler, SchedulerState.DisposeAwaiter<BoundedServices>, Scannable {

	static final Logger LOGGER = Loggers.getLogger(ThreadPerTaskBoundedElasticScheduler.class);

	final int maxThreads;
	final int maxTasksQueuedPerThread;

	final ThreadFactory factory;

	volatile SchedulerState<BoundedServices>                                                      state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ThreadPerTaskBoundedElasticScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(ThreadPerTaskBoundedElasticScheduler.class, SchedulerState.class, "state");

	private static final SchedulerState<BoundedServices> INIT =
			SchedulerState.init(SHUTDOWN);



	/**
	 * Create a {@link ThreadPerTaskBoundedElasticScheduler} with the given configuration. Note that backing threads
	 * (or executors) can be shared by each {@link reactor.core.scheduler.Scheduler.Worker}, so each worker
	 * can contribute to the task queue size.
	 *
	 * @param maxThreads             the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTasksQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param threadFactory                the {@link ThreadFactory} to name the backing threads
	 */
	ThreadPerTaskBoundedElasticScheduler(int maxThreads, int maxTasksQueuedPerThread, ThreadFactory threadFactory) {
		if (maxThreads <= 0) {
			throw new IllegalArgumentException("maxThreads must be strictly positive, was " + maxThreads);
		}
		if (maxTasksQueuedPerThread <= 0) {
			throw new IllegalArgumentException("maxTasksQueuedPerThread must be strictly positive, was " + maxTasksQueuedPerThread);
		}
		this.maxThreads = maxThreads;
		this.maxTasksQueuedPerThread = maxTasksQueuedPerThread;
		this.factory = threadFactory;

		STATE.lazySet(this, INIT);
	}

	@Override
	public boolean isDisposed() {
		// we only consider disposed as actually shutdown
		SchedulerState<BoundedServices> current = this.state;
		return current != INIT && current.currentResource == SHUTDOWN;
	}

	@Override
	public void init() {
		for (;;) {
			SchedulerState<BoundedServices> a = this.state;
			if (a != INIT) {
				if (a.currentResource == SHUTDOWN) {
					throw new IllegalStateException(
							"Initializing a disposed scheduler is not permitted");
				}
				// return early - scheduler already initialized
				return;
			}

			SchedulerState<BoundedServices> b = SchedulerState.init(new BoundedServices(this));
			if (!STATE.compareAndSet(this, INIT, b)) {
				return;
			}
		}
	}

	@Override
	public void start() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean await(BoundedServices boundedServices, long timeout, TimeUnit timeUnit) throws InterruptedException {
		if (!boundedServices.sharedDelayedTasksScheduler.awaitTermination(timeout, timeUnit)) {
			return false;
		}
		for (SequentialThreadPerTaskExecutor bs : boundedServices.activeExecutorsState.array) {
			if (!bs.await(timeout, timeUnit)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void dispose() {
		SchedulerState<BoundedServices> previous = state;

		if (previous.currentResource == SHUTDOWN) {
			// A dispose process might be ongoing, but we want a forceful shutdown,
			// so we do our best to release the resources without updating the state.
			if (previous.initialResource != null) {
				previous.initialResource.sharedDelayedTasksScheduler.shutdownNow();
				for (SequentialThreadPerTaskExecutor bs : previous.initialResource.activeExecutorsState.array) {
					bs.shutdown(true);
				}
			}
			return;
		}

		final SequentialThreadPerTaskExecutor[] toAwait = previous.currentResource.dispose();
		SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
				previous.currentResource,
				SHUTDOWN, this
		);

		STATE.compareAndSet(this, previous, shutDown);
		// If unsuccessful - either another thread disposed or restarted - no issue,
		// we only care about the one stored in shutDown.

		assert shutDown.initialResource != null;
		shutDown.initialResource.sharedDelayedTasksScheduler.shutdownNow();
		for (SequentialThreadPerTaskExecutor bs : toAwait) {
			bs.shutdown(true);
		}
	}

	@Override
	public Mono<Void> disposeGracefully() {
		return Mono.defer(() -> {
			SchedulerState<BoundedServices> previous = state;

			if (previous.currentResource == SHUTDOWN) {
				return previous.onDispose;
			}

			final SequentialThreadPerTaskExecutor[] toAwait = previous.currentResource.dispose();
			SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
					previous.currentResource,
					SHUTDOWN, this
			);

			STATE.compareAndSet(this, previous, shutDown);
			// If unsuccessful - either another thread disposed or restarted - no issue,
			// we only care about the one stored in shutDown.

			assert shutDown.initialResource != null;
			shutDown.initialResource.sharedDelayedTasksScheduler.shutdown();
			for (SequentialThreadPerTaskExecutor bs : toAwait) {
				bs.shutdown(false);
			}
			return shutDown.onDispose;
		});
	}

	@Override
	public Disposable schedule(Runnable task) {
		//tasks running once will call dispose on the SingleThreadExecutor, decreasing its usage by one
		SequentialThreadPerTaskExecutor picked = state.currentResource.pickOrAllocate();
		try {
			return picked.schedule(task, null);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the SingleThreadExecutor so it can be reused
			picked.dispose();
			throw ex;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		//tasks running once will call dispose on the SingleThreadExecutor, decreasing its usage by one
		final SequentialThreadPerTaskExecutor picked = state.currentResource.pickOrAllocate();
		try {
			return picked.schedule(task, delay, unit, null);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the SingleThreadExecutor so it can be reused
			picked.dispose();
			throw ex;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		final SequentialThreadPerTaskExecutor picked = state.currentResource.pickOrAllocate();
		try {
			return picked.schedulePeriodically(task,
					initialDelay,
					period,
					unit,
					null);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the SingleThreadExecutor so it can be reused
			picked.dispose();
			throw ex;
		}
	}

	@Override
	public String toString() {
		StringBuilder ts = new StringBuilder(Schedulers.BOUNDED_ELASTIC)
				.append('(');
		if (factory instanceof ReactorThreadFactory) {
			ts.append('\"').append(((ReactorThreadFactory) factory).get()).append("\",");
		}
		ts.append("maxThreads=").append(maxThreads)
		  .append(",maxTasksQueuedPerThread=").append(
				  maxTasksQueuedPerThread == Integer.MAX_VALUE ? "unbounded" :
						  maxTasksQueuedPerThread)
		  .append(",ttl=");
		return ts.toString();
	}

	/**
	 * @return a best effort total count of the spinned up executors
	 */
	int estimateSize() {
		return state.currentResource.activeExecutorsState.array.length;
	}

	/**
	 * Best effort snapshot of the remaining queue capacity for pending tasks across all the backing executors.
	 *
	 * @return the total task capacity, or {@literal -1} if any backing executor's task queue size cannot be instrumented
	 */
	int estimateRemainingTaskCapacity() {
		if (maxTasksQueuedPerThread == Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}

		SequentialThreadPerTaskExecutor[] busyArray = state.currentResource.activeExecutorsState.array;

		long totalTaskCapacity = maxTasksQueuedPerThread * (long) maxThreads;

		for (SequentialThreadPerTaskExecutor state : busyArray) {
			int stateQueueSize = state.estimateQueueSize();
			if (stateQueueSize >= 0) {
				totalTaskCapacity -= stateQueueSize;
			}
			else {
				return -1;
			}
		}

		return (int) Math.min(totalTaskCapacity, Integer.MAX_VALUE);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.BUFFERED) return estimateSize();
		if (key == Attr.CAPACITY) return maxThreads;
		if (key == Attr.NAME) return this.toString();

		return null;
	}

	@Override
	public Stream<? extends Scannable> inners() {
		BoundedServices services = state.currentResource;
		return Stream.of(services.activeExecutorsState.array)
		             .filter(obj -> obj != null && obj != CREATING);
	}

	@Override
	public Worker createWorker() {
		return new SingleThreadExecutorWorker(state.currentResource.pickOrAllocate());
	}

	static final class BoundedServices {

		static final class ActiveExecutorsState {
			final SequentialThreadPerTaskExecutor[] array;
			final boolean                           shutdown;

			public ActiveExecutorsState(SequentialThreadPerTaskExecutor[] array, boolean shutdown) {
				this.array = array;
				this.shutdown = shutdown;
			}
		}

		static final ActiveExecutorsState     INITIAL      = new ActiveExecutorsState(new SequentialThreadPerTaskExecutor[0], false);
		static final ActiveExecutorsState     ALL_SHUTDOWN = new ActiveExecutorsState(new SequentialThreadPerTaskExecutor[0], true);
		static final ScheduledExecutorService DELAYED_TASKS_SCHEDULER_SHUTDOWN;

		static final BoundedServices SHUTDOWN;
		static final BoundedServices                 SHUTTING_DOWN;
		static final SequentialThreadPerTaskExecutor CREATING;

		static {
			DELAYED_TASKS_SCHEDULER_SHUTDOWN = Executors.newSingleThreadScheduledExecutor();
			DELAYED_TASKS_SCHEDULER_SHUTDOWN.shutdownNow();

			SHUTDOWN = new BoundedServices();
			SHUTTING_DOWN = new BoundedServices();
			SHUTDOWN.dispose();
			SHUTTING_DOWN.dispose();

			CREATING = new SequentialThreadPerTaskExecutor(SHUTDOWN, false) {
				@Override
				public String toString() {
					return "CREATING SingleThreadExecutor";
				}
			};
			CREATING.markCount = -1; //always -1, ensures tryPick never returns true
		}

		static final AtomicLong    DELAYED_TASKS_SCHEDULER_COUNTER = new AtomicLong();
		static final ThreadFactory DELAYED_TASKS_SCHEDULER_FACTORY = r -> {
			Thread t = new Thread(r,
					Schedulers.LOOM_BOUNDED_ELASTIC + "-delayed-tasks-scheduler-" + DELAYED_TASKS_SCHEDULER_COUNTER.incrementAndGet());
			t.setDaemon(true);
			return t;
		};

		final ThreadPerTaskBoundedElasticScheduler parent;
		final ScheduledExecutorService             sharedDelayedTasksScheduler;
		final ThreadFactory factory;
		final int maxTasksQueuedPerThread;

		volatile ActiveExecutorsState activeExecutorsState;
		static final AtomicReferenceFieldUpdater<BoundedServices, ActiveExecutorsState> ACTIVE_EXECUTORS_STATE =
			AtomicReferenceFieldUpdater.newUpdater(BoundedServices.class, ActiveExecutorsState.class,
					"activeExecutorsState");

		//constructor for SHUTDOWN
		private BoundedServices() {
			this.parent = null;
			this.maxTasksQueuedPerThread = 0;
			this.factory = null;
			this.activeExecutorsState = ALL_SHUTDOWN;
			this.sharedDelayedTasksScheduler = DELAYED_TASKS_SCHEDULER_SHUTDOWN;
		}

		BoundedServices(ThreadPerTaskBoundedElasticScheduler parent) {
			this.parent = parent;
			this.maxTasksQueuedPerThread = parent.maxTasksQueuedPerThread;
			this.factory = parent.factory;
			this.sharedDelayedTasksScheduler = new ScheduledThreadPoolExecutor(1, DELAYED_TASKS_SCHEDULER_FACTORY);

			ACTIVE_EXECUTORS_STATE.lazySet(this, INITIAL);
		}

		void remove(SequentialThreadPerTaskExecutor sequentialThreadPerTaskExecutor) {
			for(;;) {
				ActiveExecutorsState current = activeExecutorsState;
				SequentialThreadPerTaskExecutor[] arr = activeExecutorsState.array;
				int len = arr.length;

				if (len == 0 || current.shutdown) {
					return;
				}

				ActiveExecutorsState replacement = null;
				if (len == 1) {
					if (arr[0] == sequentialThreadPerTaskExecutor) {
						replacement = INITIAL;
					}
				}
				else {
					for (int i = 0; i < len; i++) {
						SequentialThreadPerTaskExecutor state = arr[i];
						if (state == sequentialThreadPerTaskExecutor) {
							replacement = new ActiveExecutorsState(
									new SequentialThreadPerTaskExecutor[len - 1], false
							);
							System.arraycopy(arr, 0, replacement.array, 0, i);
							System.arraycopy(arr, i + 1, replacement.array, i, len - i - 1);
							break;
						}
					}
				}
				if (replacement == null) {
					//bounded state not found, ignore
					return;
				}
				if (ACTIVE_EXECUTORS_STATE.compareAndSet(this, current, replacement)) {
					return;
				}
			}
		}

		SequentialThreadPerTaskExecutor pickOrAllocate() {
			for (;;) {
				ActiveExecutorsState activeState = activeExecutorsState;
				if (activeState == ALL_SHUTDOWN || activeState.shutdown) {
					return CREATING; //synonym for shutdown, since the underlying executor is shut down
				}

				SequentialThreadPerTaskExecutor[] arr = activeState.array;
				int len = arr.length;

				if (len < parent.maxThreads) {
					//try to build a new resource
					SequentialThreadPerTaskExecutor
							newExecutor = new SequentialThreadPerTaskExecutor(this, true);

					SequentialThreadPerTaskExecutor[] replacement = new SequentialThreadPerTaskExecutor[len + 1];
					System.arraycopy(arr, 0, replacement, 0, len);
					replacement[len] = newExecutor;
					if (ACTIVE_EXECUTORS_STATE.compareAndSet(this, activeState, new ActiveExecutorsState(replacement, false))) {
						return newExecutor;
					}
					//else optimistically retry (implicit continue here)
				}
				else {
					SequentialThreadPerTaskExecutor choice = arr[0];
					int leastBusy = Integer.MAX_VALUE;

					for (int i = 0; i < len; i++) {
						SequentialThreadPerTaskExecutor state = arr[i];
						int busy = state.markCount;
						if (busy < leastBusy) {
							leastBusy = busy;
							choice = state;
						}
					}

					if (choice.markPicked()) {
						return choice;
					}
					//else optimistically retry (implicit continue here)
				}
			}
		}

		public SequentialThreadPerTaskExecutor[] dispose() {
			ActiveExecutorsState current;
			for (;;) {
				current = activeExecutorsState;

				if (current.shutdown) {
					return current.array;
				}

				if (ACTIVE_EXECUTORS_STATE.compareAndSet(this,
						current, new ActiveExecutorsState(current.array, true))) {
					break;
				}
			}

			return Arrays.copyOf(current.array, current.array.length);
		}
	}

	static class SequentialThreadPerTaskExecutor extends CountDownLatch implements Disposable, Scannable {

		static final int DISPOSED_STATE     = 0b1000_0000_0000_0000_0000_0000_0000_0000;
		static final int DISPOSED_NOW_STATE = 0b1100_0000_0000_0000_0000_0000_0000_0000;

		final BoundedServices parent;

		final int queueCapacity;

		final Queue<SchedulerTask>     tasksQueue;
		final ScheduledExecutorService scheduledTasksExecutor;

		volatile int markCount;
		static final AtomicIntegerFieldUpdater<SequentialThreadPerTaskExecutor> MARK_COUNT = AtomicIntegerFieldUpdater.newUpdater(
				SequentialThreadPerTaskExecutor.class, "markCount");

		volatile int size;
		static final AtomicIntegerFieldUpdater<SequentialThreadPerTaskExecutor> SIZE =
				AtomicIntegerFieldUpdater.newUpdater(SequentialThreadPerTaskExecutor.class, "size");

		volatile int state;
		static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup()
				                     .findVarHandle(SequentialThreadPerTaskExecutor.class, "state", Integer.TYPE);
			}
			catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		final ThreadFactory factory;

		SchedulerTask activeTask;

		SequentialThreadPerTaskExecutor(BoundedServices parent, boolean markPicked) {
			super(1);
			this.parent = parent;
			this.tasksQueue = Queues.<SchedulerTask>unboundedMultiproducer().get();
			this.queueCapacity = parent.maxTasksQueuedPerThread;
			this.factory = parent.factory;
			this.scheduledTasksExecutor = parent.sharedDelayedTasksScheduler;

			if (markPicked) {
				MARK_COUNT.lazySet(this, 1);
			}
		}

		void ensureQueueCapacityAndAddTasks(int taskCount) {
			if (queueCapacity == Integer.MAX_VALUE) {
				return;
			}

			for (; ; ) {
				int queueSize = size;
				int nextQueueSize = queueSize + taskCount;
				if (nextQueueSize > queueCapacity) {
					throw Exceptions.failWithRejected(
							"Task capacity of bounded elastic scheduler reached while scheduling " + taskCount + " tasks (" + nextQueueSize + "/" + queueCapacity + ")");
				}

				if (SIZE.compareAndSet(this, queueSize, nextQueueSize)) {
					return;
				}
			}
		}

		void removeTask() {
			if (queueCapacity == Integer.MAX_VALUE) {
				return;
			}

			SIZE.decrementAndGet(this);
		}

		int estimateQueueSize() {
			return size;
		}

		/**
		 * Try to mark this {@link SequentialThreadPerTaskExecutor} as picked.
		 *
		 * @return true if this state could atomically be marked as picked, false if
		 * eviction started on it in the meantime
		 */
		boolean markPicked() {
			int previousState = retain(this);

			return !isDisposed(previousState);
		}

		/**
		 * Release the {@link SequentialThreadPerTaskExecutor}, ie atomically decrease the counter of times it has been picked
		 * and mark as idle if that counter reaches 0.
		 * This is called when a worker is done using the executor. {@link #dispose()} is an alias
		 * to this method (for APIs that take a {@link Disposable}).
		 *
		 * @see #shutdown(boolean)
		 * @see #dispose()
		 */
		void release() {
			int previousState = releaseAndTryMarkDisposed(this);
			if (isDisposedNow(previousState)) {
				return;
			}

			if (isDisposed(previousState) || previousState == 1) {
				parent.remove(this);

				if (markShutdown(this) == 0) {
					clearAllTask();
					countDown();
				}
			}
		}

		/**
		 * Forcibly shut down the executor and mark that {@link SequentialThreadPerTaskExecutor} as evicted.
		 *
		 * @see #release()
		 * @see #dispose()
		 */
		void shutdown(boolean now) {
			if (!markDisposed(this, now)) {
				return;
			}

			int previousState = markShutdown(this);
			if (previousState != 0) {
				return;
			}

			drain();
		}

		/**
		 * An alias for {@link #release()}.
		 */
		@Override
		public void dispose() {
			this.release();
		}

		Disposable schedule(Runnable task, @Nullable Composite disposables) {
			if (isShutdown(this.state)) {
				throw Exceptions.failWithRejected();
			}

			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			boolean isDirect = disposables == null;
			SchedulerTask disposable =
					new SchedulerTask(this, decoratedTask, -1, TimeUnit.NANOSECONDS, disposables);

			if (!isDirect) {
				if (!disposables.add(disposable)) {
					throw Exceptions.failWithRejected();
				}
			}

			this.tasksQueue.offer(disposable);

			trySchedule();

			return disposable;
		}

		Disposable schedule(Runnable task, long delay, TimeUnit unit, @Nullable Composite disposables) {
			Objects.requireNonNull(unit, "TimeUnit should be non-null");

			if (isShutdown(this.state)) {
				throw Exceptions.failWithRejected();
			}

			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			boolean isDirect = disposables == null;
			SchedulerTask disposable =
					new SchedulerTask(this, decoratedTask, -1, unit, disposables);

			if (!isDirect) {
				if (!disposables.add(disposable)) {
					throw Exceptions.failWithRejected();
				}
			}

			if (delay <= 0) {
				this.tasksQueue.offer(disposable);

				trySchedule();
			}
			else {
				disposable.schedule(delay, unit);
			}

			return disposable;
		}

		Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit,
				@Nullable Composite disposables) {
			Objects.requireNonNull(unit, "TimeUnit should be non-null");

			if (isShutdown(this.state)) {
				throw Exceptions.failWithRejected();
			}

			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			boolean isDirect = disposables == null;

			SchedulerTask disposable = new SchedulerTask(this,
					decoratedTask,
					period < 0 ? 0 : period,
					unit,
					disposables);

			if (!isDirect) {
				if (!disposables.add(disposable)) {
					throw Exceptions.failWithRejected();
				}
			}

			if (initialDelay <= 0) {
				this.tasksQueue.offer(disposable);

				trySchedule();
			}
			else {
				disposable.schedule(initialDelay, unit);
			}

			return disposable;
		}

		void trySchedule() {
			int previousState = addWork(this);
			if (previousState != 0) {
				return;
			}

			drain();
		}

		void tryForceSchedule() {
			int previousState = forceAddWork(this);
			if (previousState != 0) {
				return;
			}

			drain();
		}

		void drain() {
			final Queue<SchedulerTask> q = this.tasksQueue;

			int m = this.state;
			for (;;) {
				for (;;) {
					if (isDisposedNow(this.markCount)) {
						clearAllTask();
						countDown();
						return;
					}

					final SchedulerTask task = q.poll();

					if (task == null) {
						break;
					}

					this.activeTask = task;
					if (task.start()) {
						return;
					}
				}

				m = markWorkDone(this, m);

				if (isShutdown(m)) {
					countDown();
					return;
				}

				if (!hasWork(m)) {
					return;
				}
			}
		}

		void clearAllTask() {
			SchedulerTask activeTask = this.activeTask;
			if (activeTask != null) {
				activeTask.dispose();
			}

			Queue<SchedulerTask> q = this.tasksQueue;

			SchedulerTask d;
			while ((d = q.poll()) != null) {
				d.dispose();
			}
		}

		/**
		 * Is this {@link SequentialThreadPerTaskExecutor} still in use by workers.
		 *
		 * @return true if in use, false if it has been disposed enough times
		 */
		@Override
		public boolean isDisposed() {
			return isDisposed(this.markCount);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		@Override
		public String toString() {
			return "SingleThreadExecutor@" + System.identityHashCode(this) + "{" + " backing=" + markCount + '}';
		}

		// -- MARK-COUNTER --

		static int retain(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				int state = instance.markCount;

				if (isDisposed(state)) {
					return state;
				}

				int nextState = state + 1;
				if (MARK_COUNT.weakCompareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static int releaseAndTryMarkDisposed(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				int state = instance.markCount;

				if (isDisposedNow(state)) {
					return state;
				}

				int nextState = isDisposed(state) || state == 1 ? DISPOSED_NOW_STATE : state - 1;
				if (MARK_COUNT.weakCompareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean markDisposed(SequentialThreadPerTaskExecutor instance, boolean now) {
			for (;;) {
				int markCount = instance.markCount;

				if (isDisposedNow(markCount) || (!now && isDisposed(markCount))) {
					return false;
				}

				if (MARK_COUNT.weakCompareAndSet(instance, markCount, DISPOSED_NOW_STATE)) {
					return true;
				}
			}
		}

		static boolean isDisposed(int state) {
			return (state & DISPOSED_STATE) == DISPOSED_STATE;
		}

		static boolean isDisposedNow(int state) {
			return (state & DISPOSED_NOW_STATE) == DISPOSED_NOW_STATE;
		}

		// -- WIP --

		static boolean isShutdown(int state) {
			return (state & Integer.MIN_VALUE) == Integer.MIN_VALUE;
		}

		static boolean hasWork(int state) {
			return (state & Integer.MAX_VALUE) > 0;
		}

		static int markWorkDone(SequentialThreadPerTaskExecutor instance, int expectedState) {
			for (;;) {
				int currentState = instance.state;

				if (expectedState != currentState) {
					return currentState;
				}

				int nextState = currentState & Integer.MIN_VALUE;
				if (STATE.weakCompareAndSetPlain(instance, currentState, nextState)) {
					return nextState;
				}
			}
		}

		static int addWork(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				int state = instance.state;

				if (isShutdown(state)) {
					return state;
				}

				int nextState = incrementWork(state);
				if (STATE.weakCompareAndSetPlain(instance, state, nextState)) {
					return state;
				}
			}
		}

		static int forceAddWork(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				int state = instance.state;
				int wip = state & Integer.MAX_VALUE;

				int nextState = (incrementWork(wip)) | (state & Integer.MIN_VALUE);
				if (STATE.weakCompareAndSetPlain(instance, state, nextState)) {
					return wip;
				}
			}
		}

		static int incrementWork(int currentWork) {
			return currentWork == Integer.MAX_VALUE ? 1 : currentWork + 1;
		}

		static int markShutdown(SequentialThreadPerTaskExecutor instance) {
			return (int) STATE.getAndBitwiseOrRelease(instance, 0b11111111111111111111111111111111);
		}
	}

	final static class SchedulerTask extends AtomicInteger
			implements Disposable, Callable<Void>, Runnable {

		static final int INITIAL_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0000;
		static final int SCHEDULED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_0001;
		static final int STARTED_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0010;
		static final int RUNNING_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0100;
		static final int COMPLETED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_1000;
		static final int DISPOSED_STATE  = 0b1000_0000_0000_0000_0000_0000_0000_0000;

		final long     fixedRatePeriod;
		final TimeUnit                        timeUnit;
		final SequentialThreadPerTaskExecutor holder;

		final Runnable task;
		@Nullable
		final Composite tracker;

		Thread carrier;
		long   startedAtInMillis;

		Future<Void> scheduledFuture;

		SchedulerTask(SequentialThreadPerTaskExecutor holder,
				Runnable task,
				long fixedRatePeriod,
				TimeUnit timeUnit,
				@Nullable Composite tracker) {
			this.fixedRatePeriod = fixedRatePeriod;
			this.timeUnit = timeUnit;
			this.holder = holder;
			this.task = task;
			this.tracker = tracker;
		}

		@Override
		public void run() {
			int previousState = markRunning(this);

			if (isDisposed(previousState)) {
				this.holder.drain();
				return;
			}

			try {
				task.run();
			}
			catch (Throwable ex) {
				if (isPeriodic()) {
					this.holder.removeTask();
				}

				if (this.tracker == null) {
					this.holder.release();
				}
				else {
					this.tracker.remove(this);
				}
				this.holder.drain();
				Schedulers.handleError(ex);
				return;
			}

			if (isPeriodic()) {
				previousState = markInitial(this);
				if (isDisposed(previousState)) {
					this.holder.removeTask();
					if (this.tracker == null) {
						this.holder.release();
					}
					this.holder.drain();
					return;
				}

				long completedAtInMillis = Instant.now().toEpochMilli();
				long executionTimeInMillis = completedAtInMillis - this.startedAtInMillis;
				long fixedRatePeriodInMillis = this.timeUnit.toMillis(this.fixedRatePeriod);

				long nextDelayInMillis = fixedRatePeriodInMillis - executionTimeInMillis;
				if (nextDelayInMillis > 0) {
					// schedule next delay
					schedule(nextDelayInMillis, TimeUnit.MILLISECONDS);
				}
				else {
					// we do not schedule task since execution time is greater than
					// fixed rate period which means we need to run this task right away
					this.holder.tasksQueue.offer(this);
				}
				// and drain next task if available
				this.holder.drain();
				return;
			}

			markCompleted(this);

			if (this.tracker == null) {
				this.holder.release();
			}
			else {
				this.tracker.remove(this);
			}

			this.holder.drain();
		}

		@Override
		public Void call() throws Exception {
			if (isDisposed()) {
				return null;
			}

			final SequentialThreadPerTaskExecutor parent = this.holder;

			this.startedAtInMillis = Instant.now().toEpochMilli();

			parent.tasksQueue.offer(this);
			parent.tryForceSchedule();
			return null;
		}

		@Override
		public void dispose() {
			int previousState = markDisposed(this);

			if (isDisposed(previousState) || isCompleted(previousState)) {
				return;
			}

			boolean isDirect = this.tracker == null;
			if (!isDirect) {
				this.tracker.remove(this);
			}

			if (isScheduled(previousState)) {
				this.scheduledFuture.cancel(true);
				this.holder.removeTask();
				if (isDirect) {
					this.holder.release();
				}
				return;
			}

			if (isRunning(previousState)) {
				this.carrier.interrupt();
				return;
			}

			if (isInitialState(previousState) || isPeriodic()) {
				this.holder.removeTask();
			}

			if (isDirect) {
				holder.release();
			}

			// don't do anything else, this task is marked as disposed so once it is
			// drained, the disposed flag should be observed and the task skipped
		}

		@Override
		public boolean isDisposed() {
			int state = get();
			return isDisposed(state) || isCompleted(state);
		}

		boolean start() {
			int previousState = markStarting(this);
			if (isDisposed(previousState)) {
				return false;
			}

			Thread carrier = this.holder.factory.newThread(this);
			this.carrier = carrier;

			if (!isPeriodic()) {
				this.holder.removeTask();
			}

			carrier.start();

			return true;
		}

		boolean isPeriodic() {
			return fixedRatePeriod >= 0;
		}

		void schedule(long delay, TimeUnit unit) {
			final ScheduledFuture<Void> future = this.holder.scheduledTasksExecutor.schedule((Callable<Void>) this, delay, unit);
			this.scheduledFuture = future;

			final int previousState = markScheduled(this);
			if (isDisposed(previousState)) {
				future.cancel(true);
			}
		}

		static boolean isInitialState(int state) {
			return state == 0;
		}

		static boolean isStarting(int state) {
			return (state & STARTED_STATE) == STARTED_STATE;
		}

		static boolean isRunning(int state) {
			return (state & RUNNING_STATE) == RUNNING_STATE;
		}

		static boolean isCompleted(int state) {
			return (state & COMPLETED_STATE) == COMPLETED_STATE;
		}

		static boolean isScheduled(int state) {
			return (state & SCHEDULED_STATE) == SCHEDULED_STATE;
		}

		static boolean isDisposed(int state) {
			return (state & DISPOSED_STATE) == DISPOSED_STATE;
		}

		static int markInitial(SchedulerTask disposable) {
			for (; ; ) {
				int state = disposable.get();

				if (isDisposed(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetPlain(state, INITIAL_STATE)) {
					return state;
				}
			}
		}

		static int markStarting(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (isDisposed(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetPlain(state, STARTED_STATE)) {
					return state;
				}
			}
		}

		static int markRunning(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (isDisposed(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetPlain(state, RUNNING_STATE)) {
					return state;
				}
			}
		}

		static int markScheduled(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (!isInitialState(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetPlain(state, SCHEDULED_STATE)) {
					return state;
				}
			}
		}

		static int markDisposed(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (isDisposed(state) || isCompleted(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetAcquire(state, state | DISPOSED_STATE)) {
					return state;
				}
			}
		}

		static void markCompleted(SchedulerTask disposable) {
			int state = disposable.get();

			if (isDisposed(state)) {
				return;
			}

			disposable.weakCompareAndSetPlain(state, COMPLETED_STATE);
		}
	}

	static class SingleThreadExecutorWorker implements Worker {

		final Composite                       disposables;
		final SequentialThreadPerTaskExecutor executor;

		SingleThreadExecutorWorker(SequentialThreadPerTaskExecutor executor) {
			this.executor = executor;
			this.disposables = Disposables.composite();
		}

		@Override
		public void dispose() {
			disposables.dispose();
			executor.release();
		}

		@Override
		public boolean isDisposed() {
			return disposables.isDisposed();
		}

		@Override
		public Disposable schedule(Runnable task) {
			return executor.schedule(task, disposables);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return executor.schedule(task, delay, unit, disposables);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return executor.schedulePeriodically(task, initialDelay, period, unit, disposables);
		}
	}

}

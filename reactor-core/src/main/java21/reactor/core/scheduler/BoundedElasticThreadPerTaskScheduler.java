/*
 * Copyright (c) 2023-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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

import static reactor.core.scheduler.BoundedElasticThreadPerTaskScheduler.BoundedServices.CREATING;
import static reactor.core.scheduler.BoundedElasticThreadPerTaskScheduler.BoundedServices.SHUTDOWN;
import static reactor.core.scheduler.BoundedElasticThreadPerTaskScheduler.BoundedServices;
import static reactor.core.scheduler.Schedulers.onSchedule;

final class BoundedElasticThreadPerTaskScheduler
		implements Scheduler, SchedulerState.DisposeAwaiter<BoundedServices>, Scannable {

	static final Logger LOGGER = Loggers.getLogger(BoundedElasticThreadPerTaskScheduler.class);

	final int maxThreads;
	final int maxTasksQueuedPerThread;

	final ThreadFactory factory;

	volatile SchedulerState<BoundedServices>                                                      state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<BoundedElasticThreadPerTaskScheduler, SchedulerState>STATE =
			AtomicReferenceFieldUpdater.newUpdater(BoundedElasticThreadPerTaskScheduler.class, SchedulerState.class, "state");

	private static final SchedulerState<BoundedServices> INIT =
			SchedulerState.init(SHUTDOWN);



	/**
	 * Create a {@link BoundedElasticThreadPerTaskScheduler} with the given configuration. Note that backing threads
	 * (or executors) can be shared by each {@link reactor.core.scheduler.Scheduler.Worker}, so each worker
	 * can contribute to the task queue size.
	 *
	 * @param maxThreads             the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTasksQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param threadFactory                the {@link ThreadFactory} to name the backing threads
	 */
	BoundedElasticThreadPerTaskScheduler(int maxThreads, int maxTasksQueuedPerThread, ThreadFactory threadFactory) {
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
	@Deprecated
	public void start() {
		throw new UnsupportedOperationException("Use init method instead");
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
						  maxTasksQueuedPerThread);
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
	 * @return the total task capacity
	 */
	int estimateRemainingTaskCapacity() {
		if (maxTasksQueuedPerThread == Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}

		SequentialThreadPerTaskExecutor[] busyArray = state.currentResource.activeExecutorsState.array;

		long numberOfTotalAvailableSlots = 0;
		for (SequentialThreadPerTaskExecutor state : busyArray) {
			numberOfTotalAvailableSlots += state.numberOfAvailableSlots();
		}

		numberOfTotalAvailableSlots += (maxThreads - busyArray.length) * (long) maxTasksQueuedPerThread;

		return (int) Math.min(numberOfTotalAvailableSlots, Integer.MAX_VALUE);
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
			CREATING.wipAndRefCnt = -1; //always -1, ensures tryPick never returns true
		}

		static final AtomicLong    DELAYED_TASKS_SCHEDULER_COUNTER = new AtomicLong();
		static final ThreadFactory DELAYED_TASKS_SCHEDULER_FACTORY = r -> {
			Thread t = new Thread(r,
					Schedulers.LOOM_BOUNDED_ELASTIC + "-delayed-tasks-scheduler-" + DELAYED_TASKS_SCHEDULER_COUNTER.incrementAndGet());
			t.setDaemon(true);
			return t;
		};

		final BoundedElasticThreadPerTaskScheduler parent;
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

		BoundedServices(BoundedElasticThreadPerTaskScheduler parent) {
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
						int busy = state.refCnt();
						if (busy < leastBusy) {
							leastBusy = busy;
							choice = state;
						}
					}

					if (choice.retain()) {
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

		final BoundedServices parent;

		final int queueCapacity;

		final Queue<SchedulerTask> tasksQueue;
		final ScheduledExecutorService scheduledTasksExecutor;

		volatile long size;
		static final AtomicLongFieldUpdater<SequentialThreadPerTaskExecutor> SIZE =
				AtomicLongFieldUpdater.newUpdater(
				SequentialThreadPerTaskExecutor.class, "size");

		volatile long wipAndRefCnt;
		static final VarHandle WIP_AND_REF_CNT;

		static {
			try {
				WIP_AND_REF_CNT = MethodHandles.lookup()
				                     .findVarHandle(SequentialThreadPerTaskExecutor.class, "wipAndRefCnt", Long.TYPE);
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
				WIP_AND_REF_CNT.set(this, 1L << 31);
			}
		}

		void incrementTasksCount() {
			for (; ; ) {
				long size = this.size;

				if (canNotAcceptTasks(size)) {
					throw Exceptions.failWithRejected();
				}

				long nextSize = size + 1;
				if (queueCapacity != Integer.MAX_VALUE && nextSize > queueCapacity) {
					throw Exceptions.failWithRejected(
							"Task capacity of bounded elastic scheduler reached while scheduling a new tasks (" + nextSize + "/" + queueCapacity + ")");
				}

				if (SIZE.compareAndSet(this, size, nextSize)) {
					return;
				}
			}
		}

		void decrementTasksCount() {
			long actualState = SIZE.decrementAndGet(this);

			if (canNotAcceptTasks(actualState) && tasksCount(actualState) == 0) {
				trySchedule();
			}
		}

		void stopAcceptingTasks() {
			for (;;) {
				long size = this.size;

				if (canNotAcceptTasks(size)) {
					return;
				}

				if (SIZE.weakCompareAndSet(this, size, size | Long.MIN_VALUE)) {
					return;
				}
			}
		}

		long numberOfEnqueuedTasks() {
			return tasksCount(this.size);
		}

		int numberOfAvailableSlots() {
			if (queueCapacity == Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			}

			return queueCapacity - (int) numberOfEnqueuedTasks();
		}

		/**
		 * Try to mark this {@link SequentialThreadPerTaskExecutor} as picked.
		 *
		 * @return true if this state could atomically be marked as picked, false if
		 * eviction started on it in the meantime
		 */
		boolean retain() {
			long previousState = retain(this);

			return !isShutdown(previousState);
		}

		/**
		 * Release the {@link SequentialThreadPerTaskExecutor}, ie atomically decrease the counter of times it has been picked
		 * and remove from the list of active executors if that counter reaches 0.
		 * This is called when a worker is done using the executor. {@link #dispose()} is an alias
		 * to this method (for APIs that take a {@link Disposable}).
		 *
		 * @see #shutdown(boolean)
		 * @see #dispose()
		 */
		void release() {
			long previousState = release(this);
			if (isShutdown(previousState)) {
				return;
			}

			if (refCnt(previousState) == 1) {
				stopAcceptingTasks();
				parent.remove(this);

				if (!hasWork(previousState)) {
					clearAllTask();
					countDown();
				}
			}
		}

		int refCnt() {
			return refCnt(this.wipAndRefCnt);
		}

		/**
		 * Forcibly shut down the executor. Can only be called from the parent
		 * {@link BoundedServices} during {@link BoundedServices#dispose()} or
		 * {@link BoundedServices#disposeGracefully()}
		 *
		 * @see #release()
		 * @see #dispose()
		 */
		void shutdown(boolean now) {
			long previousState = markShutdown(this, now);

			if (isShutdown(previousState)) {
				return;
			}

			stopAcceptingTasks();

			if (hasWork(previousState)) {
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
			incrementTasksCount();

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

			incrementTasksCount();

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

			incrementTasksCount();

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

			if (period <= 0) {
				if (initialDelay <= 0) {
					this.tasksQueue.offer(disposable);

					trySchedule();
				}
				else {
					disposable.schedule(initialDelay, unit);
				}
			}
			else {
				disposable.scheduleAtFixedRate(initialDelay, period, unit);
			}

			return disposable;
		}

		/**
		 * Is being called from public API and tries to add work and then execute it
		 */
		void trySchedule() {
			long previousState = addWork(this);
			if (hasWork(previousState) || isShutdownNow(previousState)) {
				return;
			}

			drain();
		}

		void drain() {
			final Queue<SchedulerTask> q = this.tasksQueue;

			long state = this.wipAndRefCnt;
			for (;;) {
				for (;;) {
					if (isShutdownNow(this.wipAndRefCnt)) {
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

				state = markWorkDone(this, state);

				if (isShutdown(state) && numberOfEnqueuedTasks() == 0) {
					countDown();
					return;
				}

				if (!hasWork(state)) {
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
			return isShutdown(this.wipAndRefCnt) && numberOfEnqueuedTasks() == 0;
		}

		boolean isShutdown() {
			return isShutdown(this.wipAndRefCnt);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (Attr.TERMINATED == key) return isDisposed();
			if (Attr.BUFFERED == key) return numberOfEnqueuedTasks();
			if (Attr.CAPACITY == key) return this.queueCapacity;
			return null;
		}

		@Override
		public String toString() {
			return "SingleThreadExecutor@" + System.identityHashCode(this) + "{" + " backing=" + refCnt(this.wipAndRefCnt) + '}';
		}

		static boolean canNotAcceptTasks(long state) {
			return (state & Long.MIN_VALUE) == Long.MIN_VALUE;
		}

		static long tasksCount(long state) {
			return state & Long.MAX_VALUE;
		}

		static final long WIP_MASK =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0111_1111_1111_1111_1111_1111_1111_1111L;
		static final long REF_CNT_MASK =
				0b0011_1111_1111_1111_1111_1111_1111_1111_1000_0000_0000_0000_0000_0000_0000_0000L;

		static final long SHUTDOWN_FLAG =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long SHUTDOWN_NOW_FLAG =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static int refCnt(long state) {
			return (int) ((state & REF_CNT_MASK) >> 31);
		}

		static long retain(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				long state = instance.wipAndRefCnt;

				if (isShutdown(state)) {
					return state;
				}

				long nextState = incrementRefCnt(state);
				if (WIP_AND_REF_CNT.weakCompareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long incrementRefCnt(long state) {
			long rawRefCnt = state & REF_CNT_MASK;
			return (rawRefCnt) == REF_CNT_MASK ? state : ((rawRefCnt >> 31) + 1) << 31 | (state &~ REF_CNT_MASK);
		}

		static long release(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				long state = instance.wipAndRefCnt;

				if (isShutdown(state)) {
					return state;
				}

				long refCnt = (state & REF_CNT_MASK) >> 31;
				long nextRefCnt = refCnt - 1;
				long nextState;
				if (nextRefCnt == 0) {
					nextState =
							incrementWork(state & ~REF_CNT_MASK | SHUTDOWN_NOW_FLAG | SHUTDOWN_FLAG);
				} else {
					nextState = (refCnt) == 0 ? state : nextRefCnt << 31 | (state & ~REF_CNT_MASK);
				}
				if (WIP_AND_REF_CNT.weakCompareAndSetPlain(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long markShutdown(SequentialThreadPerTaskExecutor instance, boolean now) {
			for (;;) {
				long state = instance.wipAndRefCnt;

				if (isShutdownNow(state) || (!now && isShutdown(state))) {
					return state;
				}

				if (WIP_AND_REF_CNT.weakCompareAndSetPlain(instance, state, state | SHUTDOWN_FLAG | (now ? SHUTDOWN_NOW_FLAG : 0))) {
					return state;
				}
			}
		}

		static boolean isShutdown(long state) {
			return (state & SHUTDOWN_FLAG) == SHUTDOWN_FLAG;
		}

		static boolean isShutdownNow(long state) {
			return (state & SHUTDOWN_NOW_FLAG) == SHUTDOWN_NOW_FLAG;
		}

		static boolean hasWork(long state) {
			return (state & WIP_MASK) > 0;
		}

		static long markWorkDone(SequentialThreadPerTaskExecutor instance, long expectedState) {
			for (;;) {
				long currentState = instance.wipAndRefCnt;

				if (expectedState != currentState) {
					return currentState;
				}

				long nextState = currentState &~ WIP_MASK;
				if (WIP_AND_REF_CNT.weakCompareAndSetPlain(instance, currentState, nextState)) {
					return nextState;
				}
			}
		}

		static long addWork(SequentialThreadPerTaskExecutor instance) {
			for (;;) {
				long state = instance.wipAndRefCnt;

				long nextState = incrementWork(state);
				if (WIP_AND_REF_CNT.weakCompareAndSetPlain(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long incrementWork(long state) {
			return ((state & WIP_MASK) == WIP_MASK ? (state &~ WIP_MASK) : state) + 1;
		}
	}

	final static class SchedulerTask extends AtomicInteger
			implements Disposable, Callable<Void>, Runnable {

		static final int INITIAL_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0000;
		static final int SCHEDULED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_0001;
		static final int STARTING_STATE  = 0b0000_0000_0000_0000_0000_0000_0000_0010;
		static final int RUNNING_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0100;
		static final int COMPLETED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_1000;
		static final int DISPOSED_FLAG   = 0b1000_0000_0000_0000_0000_0000_0000_0000;
		static final int HAS_FUTURE_FLAG = 0b0000_1000_0000_0000_0000_0000_0000_0000;

		final long     fixedRatePeriod;
		final TimeUnit                        timeUnit;
		final SequentialThreadPerTaskExecutor holder;

		final Runnable task;
		@Nullable
		final Composite tracker;

		Thread carrier;

		Future<?> scheduledFuture;

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
				boolean handled = false;
				try {
					Schedulers.handleError(ex);
					handled = true;
				}
				finally {
					if (!handled) {
						if (isPeriodic()) {
							this.holder.decrementTasksCount();
						}

						if (this.tracker == null) {
							this.holder.release();
						}
						else {
							this.tracker.remove(this);
						}
						this.holder.drain();
					}
				}
			}


			if (isPeriodic()) {
				boolean isInstant = this.fixedRatePeriod == 0;

				previousState = isInstant ? markInitial(this) : markRescheduled(this);
				boolean isDisposed = isDisposed(previousState);
				boolean isShutdown = holder.isShutdown();

				if (isInstant) {
					if (!isDisposed && !isShutdown) {
						// we do not schedule task since execution time is greater than
						// fixed rate period which means we need to run this task right away
						this.holder.tasksQueue.offer(this);
					}
				}

				if (isDisposed || isShutdown) {
					this.holder.decrementTasksCount();
					if (this.tracker == null) {
						this.holder.release();
					}
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
		public Void call() {
			if (isDisposed()) {
				return null;
			}

			final SequentialThreadPerTaskExecutor parent = this.holder;

			parent.tasksQueue.offer(this);
			parent.trySchedule();
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
				this.holder.decrementTasksCount();
				if (isDirect) {
					this.holder.release();
				}
				return;
			}

			if (isRunning(previousState)) {
				if (hasFuture(previousState)) {
					this.scheduledFuture.cancel(true);
				}
				this.carrier.interrupt();
				return;
			}

			if (isInitialState(previousState)) {
				this.holder.decrementTasksCount();
			}
			else if (isPeriodic()) {
				if (hasFuture(previousState)) {
					this.scheduledFuture.cancel(true);
				}
				this.holder.decrementTasksCount();
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
				this.holder.decrementTasksCount();
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

		void scheduleAtFixedRate(long initialDelay, long delay, TimeUnit unit) {
			final ScheduledFuture<?> future =
					this.holder.scheduledTasksExecutor.scheduleAtFixedRate(this::call, initialDelay, delay, unit);
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
			return (state & STARTING_STATE) == STARTING_STATE;
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
			return (state & DISPOSED_FLAG) == DISPOSED_FLAG;
		}

		static boolean hasFuture(int state) {
			return (state & HAS_FUTURE_FLAG) == HAS_FUTURE_FLAG;
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

				if (disposable.weakCompareAndSetPlain(state, (state & HAS_FUTURE_FLAG) | STARTING_STATE)) {
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

				if (disposable.weakCompareAndSetPlain(state, (state & HAS_FUTURE_FLAG) | RUNNING_STATE)) {
					return state;
				}
			}
		}

		static int markScheduled(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (isDisposed(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetRelease(state, !isInitialState(state) ? HAS_FUTURE_FLAG : SCHEDULED_STATE | HAS_FUTURE_FLAG)) {
					return state;
				}
			}
		}

		static int markRescheduled(SchedulerTask disposable) {
			for (;;) {
				int state = disposable.get();

				if (isDisposed(state)) {
					return state;
				}

				if (disposable.weakCompareAndSetRelease(state, HAS_FUTURE_FLAG | SCHEDULED_STATE)) {
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

				if (disposable.weakCompareAndSetAcquire(state, state | DISPOSED_FLAG)) {
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

		@Override
		public String toString() {
			return (isPeriodic() ? this.fixedRatePeriod == 0 ?  "InstantPeriodic" :
					"Periodic" :
					"") +
					"SchedulerTask(" + hashCode() +"){" + "carrier=" + carrier + ", " +
					"scheduledFuture=" + scheduledFuture + ", state= " + Integer.toBinaryString(get()) + '}';
		}
	}

	static class SingleThreadExecutorWorker implements Worker, Disposable, Scannable {

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

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) return disposables.size();
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.NAME) return "SingleThreadExecutorWorker";

			return executor.scanUnsafe(key);
		}
	}

}

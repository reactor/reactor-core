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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

import static reactor.core.scheduler.LoomBoundedElasticScheduler.BoundedServices.CREATING;
import static reactor.core.scheduler.LoomBoundedElasticScheduler.BoundedServices.SHUTDOWN;
import static reactor.core.scheduler.LoomBoundedElasticScheduler.BoundedServices;
import static reactor.core.scheduler.Schedulers.onSchedule;

final class LoomBoundedElasticScheduler
		implements Scheduler, SchedulerState.DisposeAwaiter<BoundedServices>, Scannable {

	static final Logger LOGGER = Loggers.getLogger(LoomBoundedElasticScheduler.class);

	final int maxThreads;
	final int maxTaskQueuedPerThread;

	final ThreadFactory factory;

	volatile SchedulerState<BoundedServices> state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<LoomBoundedElasticScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(LoomBoundedElasticScheduler.class, SchedulerState.class, "state");

	private static final SchedulerState<BoundedServices> INIT =
			SchedulerState.init(SHUTDOWN);



	/**
	 * Create a {@link LoomBoundedElasticScheduler} with the given configuration. Note that backing threads
	 * (or executors) can be shared by each {@link reactor.core.scheduler.Scheduler.Worker}, so each worker
	 * can contribute to the task queue size.
	 *
	 * @param maxThreads             the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTaskQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param threadFactory                the {@link ThreadFactory} to name the backing threads
	 */
	LoomBoundedElasticScheduler(int maxThreads, int maxTaskQueuedPerThread, ThreadFactory threadFactory) {
		if (maxThreads <= 0) {
			throw new IllegalArgumentException("maxThreads must be strictly positive, was " + maxThreads);
		}
		if (maxTaskQueuedPerThread <= 0) {
			throw new IllegalArgumentException("maxTaskQueuedPerThread must be strictly positive, was " + maxTaskQueuedPerThread);
		}
		this.maxThreads = maxThreads;
		this.maxTaskQueuedPerThread = maxTaskQueuedPerThread;
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
		SchedulerState<BoundedServices> a = this.state;
		if (a != INIT) {
			if (a.currentResource == SHUTDOWN) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
			}
			// return early - scheduler already initialized
			return;
		}

		SchedulerState<BoundedServices> b =
				SchedulerState.init(new BoundedServices(this));
		if (!STATE.compareAndSet(this, INIT, b)) {
			// Currently, isDisposed() is true for non-initialized state, but that will
			// be fixed in 3.5.0. At this stage we know however that the state is no
			// longer INIT, so isDisposed() actually means disposed state.
			if (isDisposed()) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
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
		for (SingleThreadExecutor bs : boundedServices.activeExecutorsState.array) {
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
				for (SingleThreadExecutor bs : previous.initialResource.activeExecutorsState.array) {
					bs.shutdown(true);
				}
			}
			return;
		}

		final SingleThreadExecutor[] toAwait = previous.currentResource.dispose();
		SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
				previous.currentResource,
				SHUTDOWN, this
		);

		STATE.compareAndSet(this, previous, shutDown);
		// If unsuccessful - either another thread disposed or restarted - no issue,
		// we only care about the one stored in shutDown.

		assert shutDown.initialResource != null;
		shutDown.initialResource.sharedDelayedTasksScheduler.shutdownNow();
		for (SingleThreadExecutor bs : toAwait) {
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

			final SingleThreadExecutor[] toAwait = previous.currentResource.dispose();
			SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
					previous.currentResource,
					SHUTDOWN, this
			);

			STATE.compareAndSet(this, previous, shutDown);
			// If unsuccessful - either another thread disposed or restarted - no issue,
			// we only care about the one stored in shutDown.

			assert shutDown.initialResource != null;
			shutDown.initialResource.sharedDelayedTasksScheduler.shutdown();
			for (SingleThreadExecutor bs : toAwait) {
				bs.shutdown(false);
			}
			return shutDown.onDispose;
		});
	}

	@Override
	public Disposable schedule(Runnable task) {
		//tasks running once will call dispose on the SingleThreadExecutor, decreasing its usage by one
		SingleThreadExecutor picked = state.currentResource.pick();
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
		final SingleThreadExecutor picked = state.currentResource.pick();
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
		final SingleThreadExecutor picked = state.currentResource.pick();
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
		  .append(",maxTaskQueuedPerThread=").append(maxTaskQueuedPerThread == Integer.MAX_VALUE ? "unbounded" : maxTaskQueuedPerThread)
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
		SingleThreadExecutor[] busyArray = state.currentResource.activeExecutorsState.array;
		int totalTaskCapacity = maxTaskQueuedPerThread * maxThreads;
		for (SingleThreadExecutor state : busyArray) {
			int stateQueueSize = state.estimateQueueSize();
			if (stateQueueSize >= 0) {
				totalTaskCapacity -= stateQueueSize;
			}
			else {
				return -1;
			}
		}
		return totalTaskCapacity;
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
		return new SingleThreadExecutorWorker(state.currentResource.pick());
	}

	static final class BoundedServices {

		static final class ActiveExecutorsState {
			final SingleThreadExecutor[] array;
			final boolean                shutdown;

			public ActiveExecutorsState(SingleThreadExecutor[] array, boolean shutdown) {
				this.array = array;
				this.shutdown = shutdown;
			}
		}

		static final ActiveExecutorsState
				                              ALL_IDLE     = new ActiveExecutorsState(new SingleThreadExecutor[0], false);
		static final ActiveExecutorsState     ALL_SHUTDOWN = new ActiveExecutorsState(new SingleThreadExecutor[0], true);
		static final ScheduledExecutorService DELAYED_TASKS_SCHEDULER_SHUTDOWN;

		static final BoundedServices SHUTDOWN;
		static final BoundedServices      SHUTTING_DOWN;
		static final SingleThreadExecutor CREATING;

		static {
			DELAYED_TASKS_SCHEDULER_SHUTDOWN = Executors.newSingleThreadScheduledExecutor();
			DELAYED_TASKS_SCHEDULER_SHUTDOWN.shutdownNow();

			SHUTDOWN = new BoundedServices();
			SHUTTING_DOWN = new BoundedServices();
			SHUTDOWN.dispose();
			SHUTTING_DOWN.dispose();

			CREATING = new SingleThreadExecutor(SHUTDOWN, false) {
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

		final LoomBoundedElasticScheduler parent;
		//duplicated Clock field from parent so that SHUTDOWN can be instantiated and partially used
		final ScheduledExecutorService    sharedDelayedTasksScheduler;
		final ThreadFactory               factory;
		final int                         maxTaskQueuedPerThread;

		volatile ActiveExecutorsState activeExecutorsState;
		static final AtomicReferenceFieldUpdater<BoundedServices, ActiveExecutorsState> ACTIVE_EXECUTORS_STATE =
			AtomicReferenceFieldUpdater.newUpdater(BoundedServices.class, ActiveExecutorsState.class,
					"activeExecutorsState");

		//constructor for SHUTDOWN
		private BoundedServices() {
			this.parent = null;
			this.maxTaskQueuedPerThread = 0;
			this.factory = null;
			this.activeExecutorsState = ALL_SHUTDOWN;
			this.sharedDelayedTasksScheduler = DELAYED_TASKS_SCHEDULER_SHUTDOWN;
		}

		BoundedServices(LoomBoundedElasticScheduler parent) {
			this.parent = parent;
			this.maxTaskQueuedPerThread = parent.maxTaskQueuedPerThread;
			this.factory = parent.factory;
			this.activeExecutorsState = ALL_IDLE;
			this.sharedDelayedTasksScheduler = Executors.newSingleThreadScheduledExecutor(
					DELAYED_TASKS_SCHEDULER_FACTORY);
		}

		/**
		 * @param bs the state to set busy
		 * @return true if the {@link SingleThreadExecutor} could be added to the busy array (ie. we're not shut down), false if shutting down
		 */
		boolean add(SingleThreadExecutor bs) {
			for (; ; ) {
				ActiveExecutorsState previous = activeExecutorsState;
				if (previous.shutdown) {
					return false;
				}

				int len = previous.array.length;
				SingleThreadExecutor[] replacement = new SingleThreadExecutor[len + 1];
				System.arraycopy(previous.array, 0, replacement, 0, len);
				replacement[len] = bs;

				if (ACTIVE_EXECUTORS_STATE.compareAndSet(this, previous,
						new ActiveExecutorsState(replacement, false))) {
					return true;
				}
			}
		}

		void remove(SingleThreadExecutor singleThreadExecutor) {
			for(;;) {
				ActiveExecutorsState current = activeExecutorsState;
				SingleThreadExecutor[] arr = activeExecutorsState.array;
				int len = arr.length;

				if (len == 0 || current.shutdown) {
					return;
				}

				ActiveExecutorsState replacement = null;
				if (len == 1) {
					if (arr[0] == singleThreadExecutor) {
						replacement = ALL_IDLE;
					}
				}
				else {
					for (int i = 0; i < len; i++) {
						SingleThreadExecutor state = arr[i];
						if (state == singleThreadExecutor) {
							replacement = new ActiveExecutorsState(
									new SingleThreadExecutor[len - 1], false
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

		/**
		 * Pick a {@link SingleThreadExecutor}, prioritizing idle ones then spinning up a new one if enough capacity.
		 * Otherwise, picks an active one by taking from a {@link PriorityQueue}. The picking is
		 * optimistically re-attempted if the picked slot cannot be marked as picked.
		 *
		 * @return the picked {@link SingleThreadExecutor}
		 */
		SingleThreadExecutor pick() {
			for (;;) {
				ActiveExecutorsState activeState = activeExecutorsState;
				if (activeState == ALL_SHUTDOWN || activeState.shutdown) {
					return CREATING; //synonym for shutdown, since the underlying executor is shut down
				}

				SingleThreadExecutor[] arr = activeState.array;
				int len = arr.length;

				if (len < parent.maxThreads) {
					//try to build a new resource
					SingleThreadExecutor newExecutor = new SingleThreadExecutor(this, true);

					SingleThreadExecutor[] replacement = new SingleThreadExecutor[len + 1];
					System.arraycopy(arr, 0, replacement, 0, len);
					replacement[len] = newExecutor;
					if (ACTIVE_EXECUTORS_STATE.compareAndSet(this, activeState, new ActiveExecutorsState(replacement, false))) {
						return newExecutor;
					}
					//else optimistically retry (implicit continue here)
				}
				else {
					SingleThreadExecutor choice = arr[0];
					int leastBusy = Integer.MAX_VALUE;

					for (int i = 0; i < len; i++) {
						SingleThreadExecutor state = arr[i];
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

		public SingleThreadExecutor[] dispose() {
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
				// the race can happen also with scheduled tasks and eviction
				// so we need to retry if shutdown transition fails
			}

			return Arrays.copyOf(current.array, current.array.length);
		}
	}

	static class SingleThreadExecutor extends CountDownLatch implements Disposable, Scannable {

		/**
		 * Constant for this counter of backed workers to reflect the given executor has
		 * been marked for eviction.
		 */
		static final int DISPOSED_STATE     = 0b1000_0000_0000_0000_0000_0000_0000_0000;
		static final int DISPOSED_NOW_STATE = 0b1100_0000_0000_0000_0000_0000_0000_0000;

		final BoundedServices parent;

		final int queueCapacity;

		final Queue<SchedulerTask>     tasksQueue;
		final ScheduledExecutorService scheduledTasksExecutor;

		volatile int                                                 markCount;
		static final AtomicIntegerFieldUpdater<SingleThreadExecutor> MARK_COUNT = AtomicIntegerFieldUpdater.newUpdater(
				SingleThreadExecutor.class, "markCount");

		volatile int                                                 size;
		static final AtomicIntegerFieldUpdater<SingleThreadExecutor> SIZE =
				AtomicIntegerFieldUpdater.newUpdater(SingleThreadExecutor.class, "size");

		volatile int state;
		static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup()
				                     .findVarHandle(SingleThreadExecutor.class, "state", Integer.TYPE);
			}
			catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		final ThreadFactory factory;

		SchedulerTask activeTask;

		SingleThreadExecutor(BoundedServices parent, boolean markPicked) {
			super(1);
			this.parent = parent;
			this.tasksQueue = Queues.<SchedulerTask>unboundedMultiproducer().get();
			this.queueCapacity = parent.maxTaskQueuedPerThread;
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
			return this.tasksQueue.size();
		}

		/**
		 * Try to mark this {@link SingleThreadExecutor} as picked.
		 *
		 * @return true if this state could atomically be marked as picked, false if
		 * eviction started on it in the meantime
		 */
		boolean markPicked() {
			int previousState = retain(this);

			return !isDisposed(previousState);
		}

		/**
		 * Release the {@link SingleThreadExecutor}, ie atomically decrease the counter of times it has been picked
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
		 * Forcibly shut down the executor and mark that {@link SingleThreadExecutor} as evicted.
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
		 * Is this {@link SingleThreadExecutor} still in use by workers.
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

		static int retain(SingleThreadExecutor instance) {
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

		static int releaseAndTryMarkDisposed(SingleThreadExecutor instance) {
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

		static boolean markDisposed(SingleThreadExecutor instance, boolean now) {
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

		static int markWorkDone(SingleThreadExecutor instance, int expectedState) {
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

		static int addWork(SingleThreadExecutor instance) {
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

		static int forceAddWork(SingleThreadExecutor instance) {
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

		static int markShutdown(SingleThreadExecutor instance) {
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
		final TimeUnit             timeUnit;
		final SingleThreadExecutor holder;

		final Runnable task;
		@Nullable
		final Composite tracker;

		Thread carrier;
		long   startedAtInMillis;

		Future<Void> scheduledFuture;

		SchedulerTask(SingleThreadExecutor holder,
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

			final SingleThreadExecutor parent = this.holder;

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

		final Composite            disposables;
		final SingleThreadExecutor executor;

		SingleThreadExecutorWorker(SingleThreadExecutor executor) {
			this.executor = executor;
			this.disposables = Disposables.composite(executor);
		}

		@Override
		public void dispose() {
			disposables.dispose();
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

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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.core.Disposable;
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

	final Clock         clock;
	final ThreadFactory factory;
	final long          ttlMillis;

	volatile SchedulerState<BoundedServices> state;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<LoomBoundedElasticScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(LoomBoundedElasticScheduler.class, SchedulerState.class, "state");

	private static final SchedulerState<BoundedServices> INIT =
			SchedulerState.init(SHUTDOWN);

	/**
	 * This constructor lets define millisecond-grained TTLs and a custom {@link Clock},
	 * which can be useful for tests.
	 */
	LoomBoundedElasticScheduler(int maxThreads, int maxTaskQueuedPerThread,
			ThreadFactory threadFactory, long ttlMillis, Clock clock) {
		if (ttlMillis <= 0) {
			throw new IllegalArgumentException("TTL must be strictly positive, was " + ttlMillis + "ms");
		}
		if (maxThreads <= 0) {
			throw new IllegalArgumentException("maxThreads must be strictly positive, was " + maxThreads);
		}
		if (maxTaskQueuedPerThread <= 0) {
			throw new IllegalArgumentException("maxTaskQueuedPerThread must be strictly positive, was " + maxTaskQueuedPerThread);
		}
		this.maxThreads = maxThreads;
		this.maxTaskQueuedPerThread = maxTaskQueuedPerThread;
		this.factory = threadFactory;
		this.clock = Objects.requireNonNull(clock, "A Clock must be provided");
		this.ttlMillis = ttlMillis;

		STATE.lazySet(this, INIT);
	}

	/**
	 * Create a {@link BoundedElasticScheduler} with the given configuration. Note that backing threads
	 * (or executors) can be shared by each {@link reactor.core.scheduler.Scheduler.Worker}, so each worker
	 * can contribute to the task queue size.
	 *
	 * @param maxThreads             the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTaskQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param factory                the {@link ThreadFactory} to name the backing threads
	 * @param ttlSeconds             the time-to-live (TTL) of idle threads, in seconds
	 */
	LoomBoundedElasticScheduler(int maxThreads, int maxTaskQueuedPerThread, ThreadFactory factory, int ttlSeconds) {
		this(maxThreads, maxTaskQueuedPerThread, factory, ttlSeconds * 1000L,
				Clock.tickSeconds(BoundedServices.ZONE_UTC));
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
		if (STATE.compareAndSet(this, INIT, b)) {
			try {
				b.currentResource.evictor.scheduleAtFixedRate(
						b.currentResource::eviction,
						ttlMillis, ttlMillis, TimeUnit.MILLISECONDS
				);
				return;
			}
			catch (RejectedExecutionException ree) {
				// The executor was most likely shut down in parallel.
				// If the state is SHUTDOWN - it's ok, no eviction schedule required;
				// If it's running - the other thread did a restart and will run its own schedule.
				// In both cases we throw an exception, as the caller of init() should
				// expect the scheduler to be running when this method returns.
				throw new IllegalStateException(
						"Scheduler disposed during initialization"
				);
			}
		}
		else {
			b.currentResource.evictor.shutdownNow();
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
		SchedulerState<BoundedServices> a = this.state;

		if (a.currentResource != SHUTDOWN) {
			return;
		}

		SchedulerState<BoundedServices> b =
				SchedulerState.init(new BoundedServices(this));
		if (STATE.compareAndSet(this, a, b)) {
			try {
				b.currentResource.evictor.scheduleAtFixedRate(
						b.currentResource::eviction,
						ttlMillis, ttlMillis, TimeUnit.MILLISECONDS
				);
				return;
			}
			catch (RejectedExecutionException ree) {
				// The executor was most likely shut down in parallel.
				// If the state is SHUTDOWN - it's ok, no eviction schedule required;
				// If it's running - the other thread did a restart and will run its own schedule.
				// In both cases we ignore it.
			}
		}

		// someone else shutdown or started successfully, free the resource
		b.currentResource.evictor.shutdownNow();
	}

	@Override
	public boolean await(BoundedServices boundedServices, long timeout, TimeUnit timeUnit) throws InterruptedException {
		if (!boundedServices.evictor.awaitTermination(timeout, timeUnit)) {
			return false;
		}
		for (BoundedState bs : boundedServices.busyStates.array) {
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
				previous.initialResource.evictor.shutdownNow();
				for (BoundedState bs : previous.initialResource.busyStates.array) {
					bs.shutdown(true);
				}
			}
			return;
		}

		final BoundedState[] toAwait = previous.currentResource.dispose();
		SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
				previous.currentResource,
				SHUTDOWN, this
		);

		STATE.compareAndSet(this, previous, shutDown);
		// If unsuccessful - either another thread disposed or restarted - no issue,
		// we only care about the one stored in shutDown.

		assert shutDown.initialResource != null;
		shutDown.initialResource.evictor.shutdownNow();
		for (BoundedState bs : toAwait) {
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

			final BoundedState[] toAwait = previous.currentResource.dispose();
			SchedulerState<BoundedServices> shutDown = SchedulerState.transition(
					previous.currentResource,
					SHUTDOWN, this
			);

			STATE.compareAndSet(this, previous, shutDown);
			// If unsuccessful - either another thread disposed or restarted - no issue,
			// we only care about the one stored in shutDown.

			assert shutDown.initialResource != null;
			shutDown.initialResource.evictor.shutdown();
			for (BoundedState bs : toAwait) {
				bs.shutdown(false);
			}
			return shutDown.onDispose;
		});
	}

	@Override
	public Disposable schedule(Runnable task) {
		//tasks running once will call dispose on the BoundedState, decreasing its usage by one
		BoundedState picked = state.currentResource.pick();
		try {
			return picked.schedule(task);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the BoundedState so it can be reused
			picked.dispose();
			throw ex;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		//tasks running once will call dispose on the BoundedState, decreasing its usage by one
		final BoundedState picked = state.currentResource.pick();
		try {
			return picked.schedule(task, delay, unit, true);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the BoundedState so it can be reused
			picked.dispose();
			throw ex;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		final BoundedState picked = state.currentResource.pick();
		try {
			return picked.schedulePeriodically(task,
					initialDelay,
					period,
					unit,
					true);
		}
		catch (RejectedExecutionException ex) {
			// ensure to free the BoundedState so it can be reused
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
		if (ttlMillis < 1000) {
			ts.append(ttlMillis).append("ms)");
		}
		else {
			ts.append(ttlMillis / 1000).append("s)");
		}
		return ts.toString();
	}

	/**
	 * @return a best effort total count of the spinned up executors
	 */
	int estimateSize() {
		return state.currentResource.get();
	}

	/**
	 * @return a best effort total count of the busy executors
	 */
	int estimateBusy() {
		return state.currentResource.busyStates.array.length;
	}

	/**
	 * @return a best effort total count of the idle executors
	 */
	int estimateIdle() {
		return state.currentResource.idleQueue.size();
	}

	/**
	 * Best effort snapshot of the remaining queue capacity for pending tasks across all the backing executors.
	 *
	 * @return the total task capacity, or {@literal -1} if any backing executor's task queue size cannot be instrumented
	 */
	int estimateRemainingTaskCapacity() {
		BoundedState[] busyArray = state.currentResource.busyStates.array;
		int totalTaskCapacity = maxTaskQueuedPerThread * maxThreads;
		for (BoundedState state : busyArray) {
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
		return Stream.concat(Stream.of(services.busyStates.array), services.idleQueue.stream())
		             .filter(obj -> obj != null && obj != CREATING);
	}

	@Override
	public Worker createWorker() {
		return state.currentResource.pick();
	}

	static final class BoundedServices extends AtomicInteger {

		static final class BusyStates {
			final BoundedState[] array;
			final boolean shutdown;

			public BusyStates(BoundedState[] array, boolean shutdown) {
				this.array = array;
				this.shutdown = shutdown;
			}
		}

		/**
		 * The {@link ZoneId} used for clocks. Since the {@link Clock} is only used to ensure
		 * TTL cleanup is executed every N seconds, the zone doesn't really matter, hence UTC.
		 *
		 * @implNote Note that {@link ZoneId#systemDefault()} isn't used since it triggers disk read,
		 * contrary to {@link ZoneId#of(String)}.
		 */
		static final ZoneId ZONE_UTC = ZoneId.of("UTC");

		static final BusyStates ALL_IDLE = new BusyStates(new BoundedState[0], false);
		static final BusyStates ALL_SHUTDOWN = new BusyStates(new BoundedState[0], true);
		static final ScheduledExecutorService EVICTOR_SHUTDOWN;

		static final BoundedServices SHUTDOWN;
		static final BoundedServices SHUTTING_DOWN;
		static final BoundedState    CREATING;

		static {
			EVICTOR_SHUTDOWN = Executors.newSingleThreadScheduledExecutor();
			EVICTOR_SHUTDOWN.shutdownNow();

			SHUTDOWN = new BoundedServices();
			SHUTTING_DOWN = new BoundedServices();
			SHUTDOWN.dispose();
			SHUTTING_DOWN.dispose();

			CREATING = new BoundedState(SHUTDOWN) {
				@Override
				public String toString() {
					return "CREATING BoundedState";
				}
			};
			CREATING.markCount = -1; //always -1, ensures tryPick never returns true
			CREATING.idleSinceTimestamp = -1; //consider evicted
		}

		static final AtomicLong EVICTOR_COUNTER = new AtomicLong();
		static final ThreadFactory EVICTOR_FACTORY = r -> {
			Thread t = new Thread(r, Schedulers.BOUNDED_ELASTIC + "-evictor-" + EVICTOR_COUNTER.incrementAndGet());
			t.setDaemon(true);
			return t;
		};


		final LoomBoundedElasticScheduler parent;
		//duplicated Clock field from parent so that SHUTDOWN can be instantiated and partially used
		final Clock                       clock;
		final ScheduledExecutorService    evictor;
		final Deque<BoundedState>         idleQueue;
		final ThreadFactory               factory;
		final int                         maxTaskQueuedPerThread;

		volatile BusyStates busyStates;
		static final AtomicReferenceFieldUpdater<BoundedServices, BoundedServices.BusyStates> BUSY_STATES =
			AtomicReferenceFieldUpdater.newUpdater(BoundedServices.class, BoundedServices.BusyStates.class, "busyStates");

		//constructor for SHUTDOWN
		private BoundedServices() {
			this.parent = null;
			this.maxTaskQueuedPerThread = 0;
			this.factory = null;
			this.clock = Clock.fixed(Instant.EPOCH, ZONE_UTC);
			this.idleQueue = new ConcurrentLinkedDeque<>();
			this.busyStates = ALL_SHUTDOWN;
			this.evictor = EVICTOR_SHUTDOWN;
		}

		BoundedServices(LoomBoundedElasticScheduler parent) {
			this.parent = parent;
			this.maxTaskQueuedPerThread = parent.maxTaskQueuedPerThread;
			this.factory = parent.factory;
			this.clock = parent.clock;
			this.idleQueue = new ConcurrentLinkedDeque<>();
			this.busyStates = ALL_IDLE;
			this.evictor = Executors.newSingleThreadScheduledExecutor(EVICTOR_FACTORY);
		}

		/**
		 * Trigger the eviction by computing the oldest acceptable timestamp and letting each {@link BoundedState}
		 * check (and potentially shutdown) itself.
		 */
		void eviction() {
			final long evictionTimestamp = parent.clock.millis();
			List<BoundedState> idleCandidates = new ArrayList<>(idleQueue);
			for (BoundedState candidate : idleCandidates) {
				if (candidate.tryEvict(evictionTimestamp, parent.ttlMillis)) {
					idleQueue.remove(candidate);
					decrementAndGet();
				}
			}
		}

		/**
		 * @param bs the state to set busy
		 * @return true if the {@link BoundedState} could be added to the busy array (ie. we're not shut down), false if shutting down
		 */
		boolean setBusy(BoundedState bs) {
			for (; ; ) {
				BoundedServices.BusyStates previous = busyStates;
				if (previous.shutdown) {
					return false;
				}

				int len = previous.array.length;
				BoundedState[] replacement = new BoundedState[len + 1];
				System.arraycopy(previous.array, 0, replacement, 0, len);
				replacement[len] = bs;

				if (BUSY_STATES.compareAndSet(this, previous,
						new BusyStates(replacement, false))) {
					return true;
				}
			}
		}

		void setIdle(BoundedState boundedState) {
			for(;;) {
				BusyStates current = busyStates;
				BoundedState[] arr = busyStates.array;
				int len = arr.length;

				if (len == 0 || current.shutdown) {
					return;
				}

				BusyStates replacement = null;
				if (len == 1) {
					if (arr[0] == boundedState) {
						replacement = ALL_IDLE;
					}
				}
				else {
					for (int i = 0; i < len; i++) {
						BoundedState state = arr[i];
						if (state == boundedState) {
							replacement = new BusyStates(
									new BoundedState[len - 1], false
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
				if (BUSY_STATES.compareAndSet(this, current, replacement)) {
					//impl. note: reversed order could lead to a race condition where state is added to idleQueue
					//then concurrently pick()ed into busyQueue then removed from same busyQueue.
					this.idleQueue.add(boundedState);
					// check whether we missed a shutdown
					if (this.busyStates.shutdown) {
						// we did, so we make sure the racing adds don't leak
						boundedState.shutdown(true);
						while ((boundedState = idleQueue.pollLast()) != null) {
							boundedState.shutdown(true);
						}
					}
					return;
				}
			}
		}

		/**
		 * Pick a {@link BoundedState}, prioritizing idle ones then spinning up a new one if enough capacity.
		 * Otherwise, picks an active one by taking from a {@link PriorityQueue}. The picking is
		 * optimistically re-attempted if the picked slot cannot be marked as picked.
		 *
		 * @return the picked {@link BoundedState}
		 */
		BoundedState pick() {
			for (;;) {
				if (busyStates == ALL_SHUTDOWN) {
					return CREATING; //synonym for shutdown, since the underlying executor is shut down
				}

				int a = get();
				if (!idleQueue.isEmpty()) {
					//try to find an idle resource
					BoundedState bs = idleQueue.pollLast();
					if (bs != null && bs.markPicked()) {
						boolean accepted = setBusy(bs);
						if (!accepted) { // shutdown in the meantime
							bs.shutdown(true);
							return CREATING;
						}
						return bs;
					}
					//else optimistically retry (implicit continue here)
				}
				else if (a < parent.maxThreads) {
					//try to build a new resource
					if (compareAndSet(a, a + 1)) {
						BoundedState newState = new BoundedState(this);
						if (newState.markPicked()) {
							boolean accepted = setBusy(newState);
							if (!accepted) { // shutdown in the meantime
								newState.shutdown(true);
								return CREATING;
							}
							return newState;
						}
					}
					//else optimistically retry (implicit continue here)
				}
				else {
					BoundedState s = choseOneBusy();
					if (s != null && s.markPicked()) {
						return s;
					}
					//else optimistically retry (implicit continue here)
				}
			}
		}

		@Nullable
		private BoundedState choseOneBusy() {
			BoundedState[] arr = busyStates.array;
			int len = arr.length;
			if (len == 0) {
				return null; //implicit retry in the pick() loop
			}
			if (len == 1) {
				return arr[0];
			}

			BoundedState choice = arr[0];
			int leastBusy = Integer.MAX_VALUE;

			for (int i = 0; i < arr.length; i++) {
				BoundedState state = arr[i];
				int busy = state.markCount;
				if (busy < leastBusy) {
					leastBusy = busy;
					choice = state;
				}
			}
			return choice;
		}

		public BoundedState[] dispose() {
			BusyStates current;
			for (;;) {
				current = busyStates;

				if (current.shutdown) {
					return current.array;
				}

				if (BUSY_STATES.compareAndSet(this,
						current, new BusyStates(current.array, true))) {
					break;
				}
				// the race can happen also with scheduled tasks and eviction
				// so we need to retry if shutdown transition fails
			}

			BoundedState[] arr = current.array;
			// The idleQueue must be drained first as concurrent removals
			// by evictor or additions by finished tasks can invalidate the size
			// used if a regular array was created here.
			// In case of concurrent calls, it is not needed to atomically drain the
			// queue, nor guarantee same BoundedStates are read, as long as the caller
			// shuts down the returned BoundedStates. Also, idle ones should easily
			// shut down, it's not necessary to distinguish graceful from forceful.
			ArrayList<BoundedState> toAwait = new ArrayList<>(idleQueue.size() + arr.length);
			BoundedState bs;
			while ((bs = idleQueue.pollLast()) != null) {
				toAwait.add(bs);
			}
			Collections.addAll(toAwait, arr);
			return toAwait.toArray(new BoundedState[0]);
		}
	}

	static class BoundedState extends CountDownLatch implements Worker, Disposable, Scannable {

		/**
		 * Constant for this counter of backed workers to reflect the given executor has
		 * been marked for eviction.
		 */
		static final int DISPOSED_STATE     = 0b1000_0000_0000_0000_0000_0000_0000_0000;
		static final int DISPOSED_NOW_STATE = 0b1100_0000_0000_0000_0000_0000_0000_0000;

		final BoundedServices parent;

		final int queueCapacity;

		final Queue<ScheduledDisposable> tasksQueue;
		final ScheduledExecutorService scheduledTasksExecutor;

		long idleSinceTimestamp = -1L;

		volatile int markCount;
		static final AtomicIntegerFieldUpdater<BoundedState> MARK_COUNT = AtomicIntegerFieldUpdater.newUpdater(BoundedState.class, "markCount");

		volatile int size;
		static final AtomicIntegerFieldUpdater<BoundedState> SIZE =
				AtomicIntegerFieldUpdater.newUpdater(BoundedState.class, "size");

		volatile int state;
		static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup()
				                     .findVarHandle(BoundedState.class, "state", Integer.TYPE);
			}
			catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		final ThreadFactory factory;

		ScheduledDisposable activeTask;

		BoundedState(BoundedServices parent) {
			super(1);
			this.parent = parent;
			this.tasksQueue = Queues.<ScheduledDisposable>unboundedMultiproducer().get();
			this.queueCapacity = parent.maxTaskQueuedPerThread;
			this.factory = parent.factory;
			this.scheduledTasksExecutor = parent.evictor;
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

		int estimateQueueSize() {
			return this.tasksQueue.size();
		}

		/**
		 * Try to mark this {@link BoundedState} as picked.
		 *
		 * @return true if this state could atomically be marked as picked, false if
		 * eviction started on it in the meantime
		 */
		boolean markPicked() {
			int previousState = retain(this);

			return !isDisposed(previousState);
		}

		/**
		 * Check if this {@link BoundedState} should be evicted by comparing its idleSince
		 * timestamp to the evictionTimestamp and comparing the difference with the
		 * given ttlMillis. When eligible for eviction, the executor is shut down and the
		 * method returns true (to remove the state from the array).
		 *
		 * @param evictionTimestamp the timestamp at which the eviction process is running
		 * @param ttlMillis the maximum idle duration
		 * @return true if this {@link BoundedState} has shut down itself as part of eviction, false otherwise
		 */
		boolean tryEvict(long evictionTimestamp, long ttlMillis) {
			long idleSince = this.idleSinceTimestamp;
			if (idleSince < 0) return false;
			long elapsed = evictionTimestamp - idleSince;
			if (elapsed >= ttlMillis) {
				if (MARK_COUNT.compareAndSet(this, 0, DISPOSED_NOW_STATE)) {
					int previousState = markShutdown(this);

					if (previousState == 0) {
						clearAllTask();
						countDown();
					}

					return true;
				}
			}
			return false;
		}

		/**
		 * Release the {@link BoundedState}, ie atomically decrease the counter of times it has been picked
		 * and mark as idle if that counter reaches 0.
		 * This is called when a worker is done using the executor. {@link #dispose()} is an alias
		 * to this method (for APIs that take a {@link Disposable}).
		 *
		 * @see #shutdown(boolean)
		 * @see #dispose()
		 */
		void release() {
			int picked = release(this);
			if (picked < 0) {
				//picked == -1 means being evicted, do nothing in that case
				return;
			}

			if (picked == 0) {
				//we released enough that this BoundedState is now idle
				this.idleSinceTimestamp = parent.clock.millis();
				parent.setIdle(this);
			}
			else {
				//still picked by at least one worker, defensively ensure timestamp is not set
				this.idleSinceTimestamp = -1L;
			}
		}

		/**
		 * Forcibly shut down the executor and mark that {@link BoundedState} as evicted.
		 *
		 * @see #release()
		 * @see #dispose()
		 */
		void shutdown(boolean now) {
			this.idleSinceTimestamp = -1L;
			MARK_COUNT.set(this, now ? DISPOSED_NOW_STATE : DISPOSED_STATE);
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

		@Override
		public Disposable schedule(Runnable task) {
			return schedule(task, false);
		}

		Disposable schedule(Runnable task, boolean direct) {
			Objects.requireNonNull(task, "Task should be non-null");
			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			ScheduledDisposable disposable =
					new ScheduledDisposable(this, decoratedTask, 0, TimeUnit.NANOSECONDS, direct);

			this.tasksQueue.offer(disposable);

			trySchedule();

			return disposable;
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return schedule(task, delay, unit, false);
		}

		Disposable schedule(Runnable task, long delay, TimeUnit unit, boolean direct) {
			Objects.requireNonNull(task, "Task should be non-null");
			Objects.requireNonNull(unit, "TimeUnit should be non-null");
			if (delay <= 0) {
				throw new IllegalArgumentException();
			}

			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			ScheduledDisposable disposable =
					new ScheduledDisposable(this, decoratedTask, 0, unit, direct);

			disposable.schedule(delay, unit);

			return disposable;
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return schedulePeriodically(task, initialDelay, period, unit, false);
		}

		Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit,
				boolean direct) {
			Objects.requireNonNull(task, "Task should be non-null");
			Objects.requireNonNull(unit, "TimeUnit should be non-null");
			if (initialDelay <= 0 || period <= 0) {
				throw new IllegalArgumentException();
			}

			ensureQueueCapacityAndAddTasks(1);

			Runnable decoratedTask = onSchedule(task);
			ScheduledDisposable disposable =
					new ScheduledDisposable(this, decoratedTask, period, unit, direct);

			disposable.schedule(initialDelay, unit);

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
			final Queue<ScheduledDisposable> q = this.tasksQueue;

			int m = this.state;
			for (;;) {
				for (;;) {
					if (isDisposedNow(this.markCount)) {
						clearAllTask();
						countDown();
						return;
					}

					final ScheduledDisposable task = q.poll();

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
			ScheduledDisposable activeTask = this.activeTask;
			if (activeTask != null) {
				activeTask.dispose();
			}

			Queue<ScheduledDisposable> q = this.tasksQueue;

			ScheduledDisposable d;
			while ((d = q.poll()) != null) {
				d.dispose();
			}
		}

		/**
		 * Is this {@link BoundedState} still in use by workers.
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
			return "BoundedState@" + System.identityHashCode(this) + "{" + " backing=" + MARK_COUNT.get(
					this) + ", idleSince=" + idleSinceTimestamp + '}';
		}

		// -- MARK-COUNTER --

		static int retain(BoundedState instance) {
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

		static int release(BoundedState instance) {
			for (;;) {
				int state = instance.markCount;

				if (isDisposed(state)) {
					return state;
				}

				int nextState = state - 1;
				if (MARK_COUNT.weakCompareAndSet(instance, state, nextState)) {
					return state;
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

		static int markWorkDone(BoundedState instance, int expectedState) {
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

		static int addWork(BoundedState instance) {
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

		static int forceAddWork(BoundedState instance) {
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

		static int markShutdown(BoundedState instance) {
			return (int) STATE.getAndBitwiseOrRelease(instance, 0b11111111111111111111111111111111);
		}
	}

	final static class ScheduledDisposable extends AtomicInteger
			implements Disposable, Callable<Void>, Runnable {

		static final int INITIAL_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0000;
		static final int SCHEDULED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_0001;
		static final int STARTED_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0010;
		static final int RUNNING_STATE   = 0b0000_0000_0000_0000_0000_0000_0000_0100;
		static final int COMPLETED_STATE = 0b0000_0000_0000_0000_0000_0000_0000_1000;
		static final int DISPOSED_STATE  = 0b1000_0000_0000_0000_0000_0000_0000_0000;

		final long         period;
		final TimeUnit     timeUnit;
		final BoundedState holder;

		final Runnable task;
		final boolean direct;

		Thread carrier;

		Future<Void> scheduledFuture;

		ScheduledDisposable(BoundedState holder,
				Runnable task,
				long period,
				TimeUnit timeUnit,
				boolean direct) {
			this.period = period;
			this.timeUnit = timeUnit;
			this.holder = holder;
			this.task = task;
			this.direct = direct;
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
			catch (Throwable t) {
				if (direct) {
					this.holder.release();
				}
				this.holder.drain();
				throw Exceptions.propagate(t);
			}

			if (isPeriodic()) {
				previousState = markInitial(this);
				if (isDisposed(previousState)) {
					if (direct) {
						this.holder.release();
					}
					this.holder.drain();
					return;
				}

				// schedule next delay
				schedule(this.period, this.timeUnit);
				// and drain next task if available
				this.holder.drain();
				return;
			}

			markCompleted(this);

			if (direct) {
				this.holder.release();
			}

			this.holder.drain();
		}

		@Override
		public Void call() throws Exception {
			if (isDisposed()) {
				return null;
			}

			holder.tasksQueue.offer(this);
			holder.tryForceSchedule();
			return null;
		}

		@Override
		public void dispose() {
			int previousState = markDisposed(this);

			if (isDisposed(previousState) || isCompleted(previousState)) {
				return;
			}

			if (isScheduled(previousState)) {
				this.scheduledFuture.cancel(true);
				if (direct) {
					holder.release();
				}
				return;
			}

			if (isRunning(previousState)) {
				this.carrier.interrupt();
				return;
			}

			if (direct) {
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

			carrier.start();

			return true;
		}

		boolean isPeriodic() {
			return period > 0;
		}

		void schedule(long delay, TimeUnit unit) {
			this.scheduledFuture =
					this.holder.scheduledTasksExecutor.schedule((Callable<Void>) this, delay, unit);

			markScheduled(this);
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

		static int markInitial(ScheduledDisposable disposable) {
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

		static int markStarting(ScheduledDisposable disposable) {
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

		static int markRunning(ScheduledDisposable disposable) {
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

		static void markScheduled(ScheduledDisposable disposable) {
			for (;;) {
				int state = disposable.get();

				if (!isInitialState(state)) {
					return;
				}

				if (disposable.weakCompareAndSetPlain(state, SCHEDULED_STATE)) {
					return;
				}
			}
		}

		static int markDisposed(ScheduledDisposable disposable) {
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

		static void markCompleted(ScheduledDisposable disposable) {
			int state = disposable.get();

			if (isDisposed(state)) {
				return;
			}

			disposable.weakCompareAndSetPlain(state, COMPLETED_STATE);
		}
	}

}

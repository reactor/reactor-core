package reactor.core.scheduler;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static reactor.core.scheduler.Schedulers.onSchedule;

public class LoomBoundedElasticScheduler implements Scheduler,
                                                    SchedulerState.DisposeAwaiter<BoundedServices>,
                                                    Scannable {

	static final Logger LOGGER = Loggers.getLogger(class);

	static final int DEFAULT_TTL_SECONDS = 60;

	static final AtomicLong COUNTER = new AtomicLong();

	final int maxThreads;
	final int maxTaskQueuedPerThread;

	final Clock         clock;
	final ThreadFactory factory;
	final long          ttlMillis;

	volatile     SchedulerState<BoundedServices> state;
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
	 * @param maxThreads the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTaskQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param factory the {@link ThreadFactory} to name the backing threads
	 * @param ttlSeconds the time-to-live (TTL) of idle threads, in seconds
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
			} catch (RejectedExecutionException ree) {
				// The executor was most likely shut down in parallel.
				// If the state is SHUTDOWN - it's ok, no eviction schedule required;
				// If it's running - the other thread did a restart and will run its own schedule.
				// In both cases we throw an exception, as the caller of init() should
				// expect the scheduler to be running when this method returns.
				throw new IllegalStateException(
						"Scheduler disposed during initialization"
				);
			}
		} else {
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
			} catch (RejectedExecutionException ree) {
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
			if (!bs.executor.awaitTermination(timeout, timeUnit)) {
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
			return Schedulers.directSchedule(picked.executor, task, picked, 0L, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException ex) {
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
			return Schedulers.directSchedule(picked.executor, task, picked, delay, unit);
		} catch (RejectedExecutionException ex) {
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
			Disposable scheduledTask = Schedulers.directSchedulePeriodically(picked.executor,
					task,
					initialDelay,
					period,
					unit);
			//a composite with picked ensures the cancellation of the task releases the BoundedState
			// (ie decreases its usage by one)
			return Disposables.composite(scheduledTask, picked);
		} catch (RejectedExecutionException ex) {
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
		BoundedState picked = state.currentResource.pick();
		ExecutorServiceWorker worker = new ExecutorServiceWorker(picked.executor);
		worker.disposables.add(picked); //this ensures the BoundedState will be released when worker is disposed
		return worker;
	}

	static final class BoundedServices extends AtomicInteger {

		static final class BusyStates {
			final BoundedState[] array;
			final boolean                                shutdown;

			public BusyStates(BoundedState[] array, boolean shutdown) {
				this.array = array;
				this.shutdown = shutdown;
			}
		}

		/**
		 * The {@link ZoneId} used for clocks. Since the {@link Clock} is only used to ensure
		 * TTL cleanup is executed every N seconds, the zone doesn't really matter, hence UTC.
		 * @implNote Note that {@link ZoneId#systemDefault()} isn't used since it triggers disk read,
		 * contrary to {@link ZoneId#of(String)}.
		 */
		static final ZoneId                       ZONE_UTC = ZoneId.of("UTC");

		static final BoundedServices.BusyStates
				                                                        ALL_IDLE     = new BoundedServices.BusyStates(new BoundedState[0], false);
		static final BoundedServices.BusyStates
				                                                        ALL_SHUTDOWN = new BoundedServices.BusyStates(new BoundedState[0], true);
		static final ScheduledExecutorService                           EVICTOR_SHUTDOWN;

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

			ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
			s.shutdownNow();
			CREATING = new BoundedState(SHUTDOWN, s) {
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


		final BoundedElasticScheduler             parent;
		//duplicated Clock field from parent so that SHUTDOWN can be instantiated and partially used
		final Clock                               clock;
		final ScheduledExecutorService                    evictor;
		final Deque<BoundedState> idleQueue;

		volatile     BoundedServices.BusyStates                                                                      busyStates;
		static final AtomicReferenceFieldUpdater<BoundedServices, BoundedServices.BusyStates>BUSY_STATES =
				AtomicReferenceFieldUpdater.newUpdater(BoundedServices.class, BoundedServices.BusyStates.class,
						"busyStates");

		//constructor for SHUTDOWN
		private BoundedServices() {
			this.parent = null;
			this.clock = Clock.fixed(Instant.EPOCH, ZONE_UTC);
			this.idleQueue = new ConcurrentLinkedDeque<>();
			this.busyStates = ALL_SHUTDOWN;
			this.evictor = EVICTOR_SHUTDOWN;
		}

		BoundedServices(BoundedElasticScheduler parent) {
			this.parent = parent;
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
						new BoundedServices.BusyStates(replacement, false))) {
					return true;
				}
			}
		}

		void setIdle(BoundedState boundedState) {
			for(;;) {
				BoundedServices.BusyStates current = busyStates;
				BoundedState[] arr = busyStates.array;
				int len = arr.length;

				if (len == 0 || current.shutdown) {
					return;
				}

				BoundedServices.BusyStates replacement = null;
				if (len == 1) {
					if (arr[0] == boundedState) {
						replacement = ALL_IDLE;
					}
				}
				else {
					for (int i = 0; i < len; i++) {
						BoundedState state = arr[i];
						if (state == boundedState) {
							replacement = new BoundedServices.BusyStates(
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
						ScheduledExecutorService s = Schedulers.decorateExecutorService(parent, parent.createBoundedExecutorService());
						BoundedState
								newState = new BoundedState(this, s);
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
			BoundedServices.BusyStates current;
			for (;;) {
				current = busyStates;

				if (current.shutdown) {
					return current.array;
				}

				if (BUSY_STATES.compareAndSet(this,
						current, new BoundedServices.BusyStates(current.array,	true))) {
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

	/**
	 * A class that encapsulate state around the {@link BoundedScheduledExecutorService} and
	 * atomically marking them picked/idle.
	 */
	static class BoundedState extends DelayQueue<ScheduledDisposable> implements Worker, Disposable,
	                                                         Scannable, Runnable {

		/**
		 * Constant for this counter of backed workers to reflect the given executor has
		 * been marked for eviction.
		 */
		static final int EVICTED = -1;

		final BoundedServices parent;

		final int queueCapacity;

		final ReentrantLock lock = new ReentrantLock();

		long idleSinceTimestamp = -1L;

		volatile int markCount;
		static final AtomicIntegerFieldUpdater<BoundedState> MARK_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(BoundedState.class, "markCount");

		volatile int wip;
		static final AtomicIntegerFieldUpdater<BoundedState> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BoundedState.class, "wip");

		static ThreadFactory factory;

		BoundedState(BoundedServices parent) {
			super();
			this.parent = parent;
			this.queueCapacity = parent.parent.maxTaskQueuedPerThread;
		}

		void ensureQueueCapacity(int taskCount) {
			if (queueCapacity == Integer.MAX_VALUE) {
				return;
			}
			int queueSize = super.size();
			if ((queueSize + taskCount) > queueCapacity) {
				throw Exceptions.failWithRejected("Task capacity of bounded elastic scheduler reached while scheduling " + taskCount + " tasks (" + (queueSize + taskCount) + "/" + queueCapacity + ")");
			}
		}

		/**
		 * @return the queue size if the executor is a {@link ScheduledThreadPoolExecutor}, -1 otherwise
		 */
		int estimateQueueSize() {
			return size();
		}

		/**
		 * Try to mark this {@link BoundedState} as picked.
		 *
		 * @return true if this state could atomically be marked as picked, false if
		 * eviction started on it in the meantime
		 */
		boolean markPicked() {
			for(;;) {
				int i = MARK_COUNT.get(this);
				if (i == EVICTED) {
					return false; //being evicted
				}
				if (MARK_COUNT.compareAndSet(this, i, i + 1)) {
					return true;
				}
			}
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
				if (MARK_COUNT.compareAndSet(this, 0, EVICTED)) {
//					executor.shutdownNow();
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
			int picked = MARK_COUNT.decrementAndGet(this);
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
			MARK_COUNT.set(this, EVICTED);
//			if (now) {
//				this.executor.shutdownNow();
//			} else {
//				this.executor.shutdown();
//			}
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
			task = onSchedule(task);

			ScheduledDisposable disposable =
					new ScheduledDisposable(0, TimeUnit.NANOSECONDS, this);

			final ReentrantLock lock = this.lock;
			lock.lock();
			try {
				ensureQueueCapacity(1);
				offer(disposable);
			} finally {
				lock.unlock();
			}

			return disposable;
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return Worker.super.schedule(task, delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return Worker.super.schedulePeriodically(task, initialDelay, period, unit);
		}


		void trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			factory.newThread()
		}

		void drain() {
			for (;;) {
				poll()
				factory.newThread().
			}
		}

		void

		/**
		 * Is this {@link BoundedState} still in use by workers.
		 *
		 * @return true if in use, false if it has been disposed enough times
		 */
		@Override
		public boolean isDisposed() {
			return MARK_COUNT.get(this) <= 0;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return Schedulers.scanExecutor(executor, key);
		}

		@Override
		public String toString() {
			return "BoundedState@" + System.identityHashCode(this) + "{" + " backing=" +MARK_COUNT.get(this) + ", idleSince=" + idleSinceTimestamp + ", executor=" + executor + '}';
		}
	}


	final static class ScheduledDisposable implements Disposable, Delayed, Runnable {

		final long delay;
		final TimeUnit timeUnit;
		final BoundedState holder;

		final Runnable task;

		Thread carrier;

		ScheduledDisposable(long delay, TimeUnit unit, BoundedState holder, Runnable task) {
			this.delay = delay;
			this.timeUnit = unit;
			this.holder = holder;
			this.task = task;
		}

		@Override
		public void run() {
			task.run();
		}

		@Override
		public int compareTo(Delayed other) {
			if (other == this) // compare zero if same object
				return 0;
			long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
			return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
		}

		@Override
		public void dispose() {
			holder.remove(this);
			carrier.interrupt();
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(delay, timeUnit);
		}
	}

}

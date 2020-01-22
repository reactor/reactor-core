/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * A {@link Scheduler} that uses a virtual clock, allowing to manipulate time
 * (eg. in tests). Can replace the default reactor schedulers by using
 * the {@link #getOrSet} / {@link #set(VirtualTimeScheduler)} methods.
 *
 * @author Stephane Maldini
 */
public class VirtualTimeScheduler implements Scheduler {

	/**
	 * Create a new {@link VirtualTimeScheduler} without enabling it. Call
	 * {@link #getOrSet(VirtualTimeScheduler)} to enable it on
	 * {@link reactor.core.scheduler.Schedulers.Factory} factories.
	 *
	 * @return a new {@link VirtualTimeScheduler} intended for timed-only
	 * {@link Schedulers} factories.
	 */
	public static VirtualTimeScheduler create() {
		return create(false);
	}

	/**
	 * Create a new {@link VirtualTimeScheduler} without enabling it. Call
	 * {@link #getOrSet(VirtualTimeScheduler)} to enable it on
	 * {@link reactor.core.scheduler.Schedulers.Factory} factories.
	 *
	 * @param defer true to defer all clock move operations until there are tasks in queue
	 *
	 * @return a new {@link VirtualTimeScheduler} intended for timed-only
	 * {@link Schedulers} factories.
	 */
	public static VirtualTimeScheduler create(boolean defer) {
		return new VirtualTimeScheduler(defer);
	}

	/**
	 * Assign a single newly created {@link VirtualTimeScheduler} to all {@link reactor.core.scheduler.Schedulers.Factory}
	 * factories. While the method is thread safe, its usually advised to execute such
	 * wide-impact BEFORE all tested code runs (setup etc). The created scheduler is returned.
	 *
	 * @return the VirtualTimeScheduler that was created and set through the factory
	 */
	public static VirtualTimeScheduler getOrSet() {
		return enable(VirtualTimeScheduler::create, false);
	}

	/**
	 * Assign an externally created {@link VirtualTimeScheduler} to the relevant
	 * {@link reactor.core.scheduler.Schedulers.Factory} factories, depending on how it was created (see
	 * {@link #create()} and {@link #create()}). Note that the returned scheduler
	 * should always be captured and used going forward, as the provided scheduler can be
	 * superseded by a matching scheduler that has already been enabled.
	 * <p>
	 * While the method is thread safe, it's usually advised to execute such wide-impact
	 * BEFORE all tested code runs (setup etc). The actual enabled Scheduler is returned.
	 *
	 * @param scheduler the {@link VirtualTimeScheduler} to use in factories.
	 * @return the enabled VirtualTimeScheduler (can be different from the provided one)
	 */
	public static VirtualTimeScheduler getOrSet(VirtualTimeScheduler scheduler) {
		return enable(() -> scheduler, false);
	}

	/**
	 * Assign an externally created {@link VirtualTimeScheduler} to the relevant
	 * {@link reactor.core.scheduler.Schedulers.Factory} factories, depending on how it was created (see
	 * {@link #create()} and {@link #create()}). Contrary to
	 * {@link #getOrSet(VirtualTimeScheduler)}, the provided scheduler is always used, even
	 * if a matching scheduler is currently enabled.
	 * <p>
	 * While the method is thread safe, it's usually advised to execute such wide-impact
	 * BEFORE all tested code runs (setup etc).
	 *
	 * @param scheduler the {@link VirtualTimeScheduler} to use in factories.
	 * @return the enabled VirtualTimeScheduler (same as provided), for chaining
	 */
	public static VirtualTimeScheduler set(VirtualTimeScheduler scheduler) {
		return enable(() -> scheduler, true);
	}

	/**
	 * Common method to enable a {@link VirtualTimeScheduler} in {@link Schedulers}
	 * factories. The supplier is lazily called. Enabling the same scheduler twice is
	 * also idempotent.
	 *
	 * @param schedulerSupplier the supplier executed to obtain a fresh {@link VirtualTimeScheduler}
	 * @return the scheduler that is actually used after the operation.
	 */
	static VirtualTimeScheduler enable(Supplier<VirtualTimeScheduler> schedulerSupplier) {
		return enable(schedulerSupplier, false);
	}

	/**
	 * Common method to enable a {@link VirtualTimeScheduler} in {@link Schedulers}
	 * factories. The supplier is lazily called. Enabling the same scheduler twice is
	 * also idempotent.
	 *
	 * @param schedulerSupplier the supplier executed to obtain a fresh {@link VirtualTimeScheduler}
	 * @param exact whether or not to force the use of the supplier, even if there's a matching scheduler
	 * @return the scheduler that is actually used after the operation.
	 */
	static VirtualTimeScheduler enable(Supplier<VirtualTimeScheduler>
			schedulerSupplier, boolean exact) {
		for (; ; ) {
			VirtualTimeScheduler s = CURRENT.get();
			if (s != null && !exact) {
				return s;
			}
			VirtualTimeScheduler newS = schedulerSupplier.get();
			if (newS == CURRENT.get()) {
				return newS; //nothing to do, it has already been set in the past
			}

			if (CURRENT.compareAndSet(s, newS)) {
				Schedulers.setFactory(new AllFactory(newS));
				if (CURRENT.get() == newS) {
					return newS;
				}
			}
		}
	}

	/**
	 * The current {@link VirtualTimeScheduler} assigned in {@link Schedulers}
	 * @return current {@link VirtualTimeScheduler} assigned in {@link Schedulers}
	 * @throws IllegalStateException if no {@link VirtualTimeScheduler} has been found
	 */
	public static VirtualTimeScheduler get(){
		VirtualTimeScheduler s = CURRENT.get();
		if (s == null) {
			throw new IllegalStateException("Check if VirtualTimeScheduler#enable has been invoked first");
		}
		return s;
	}

	/**
	 * Return true if there is a {@link VirtualTimeScheduler} currently used by the
	 * {@link Schedulers} factory (ie it has been {@link #set(VirtualTimeScheduler) enabled}),
	 * false otherwise (ie it has been {@link #reset() reset}).
	 */
	public static boolean isFactoryEnabled() {
		return CURRENT.get() != null;
	}

	/**
	 * Re-assign the default Reactor Core {@link Schedulers} factories.
	 * While the method is thread safe, its usually advised to execute such wide-impact
	 * AFTER all tested code has been run (teardown etc).
	 */
	public static void reset() {
		VirtualTimeScheduler s = CURRENT.get();
		if (s != null && CURRENT.compareAndSet(s, null)) {
			Schedulers.resetFactory();
		}
	}

	final Queue<TimedRunnable> queue =
			new PriorityBlockingQueue<>(Queues.XS_BUFFER_SIZE);

	@SuppressWarnings("unused")
	volatile long counter;

	volatile long nanoTime;

	volatile long deferredNanoTime;
	static final AtomicLongFieldUpdater<VirtualTimeScheduler> DEFERRED_NANO_TIME = AtomicLongFieldUpdater.newUpdater(VirtualTimeScheduler.class, "deferredNanoTime");

	volatile int advanceTimeWip;
	static final AtomicIntegerFieldUpdater<VirtualTimeScheduler> ADVANCE_TIME_WIP =
			AtomicIntegerFieldUpdater.newUpdater(VirtualTimeScheduler.class, "advanceTimeWip");

	volatile boolean shutdown;

	final boolean defer;

	final VirtualTimeWorker directWorker;

	protected VirtualTimeScheduler(boolean defer) {
		this.defer = defer;
		directWorker = createWorker();
	}

	/**
	 * Triggers any tasks that have not yet been executed and that are scheduled to be
	 * executed at or before this {@link VirtualTimeScheduler}'s present time.
	 */
	public void advanceTime() {
		advanceTimeBy(Duration.ZERO);
	}

	/**
	 * Moves the {@link VirtualTimeScheduler}'s clock forward by a specified amount of time.
	 *
	 * @param delayTime the amount of time to move the {@link VirtualTimeScheduler}'s clock forward
	 */
	public void advanceTimeBy(Duration delayTime) {
		advanceTime(delayTime.toNanos());
	}

	/**
	 * Moves the {@link VirtualTimeScheduler}'s clock to a particular moment in time.
	 *
	 * @param instant the point in time to move the {@link VirtualTimeScheduler}'s
	 * clock to
	 */
	public void advanceTimeTo(Instant instant) {
		long targetTime = TimeUnit.NANOSECONDS.convert(instant.toEpochMilli(),
				TimeUnit.MILLISECONDS);
		advanceTime(targetTime - nanoTime);
	}

	/**
	 * Get the number of scheduled tasks.
	 * <p>
	 * This count includes tasks that have already performed as well as ones scheduled in future.
	 * For periodical task, initial task is first scheduled and counted as one. Whenever
	 * subsequent repeat happens this count gets incremented for the one that is scheduled
	 * for the next run.
	 *
	 * @return number of tasks that have scheduled on this scheduler.
	 */
	public long getScheduledTaskCount() {
		return this.counter;
	}

	@Override
	public VirtualTimeWorker createWorker() {
		if (shutdown) {
			throw new IllegalStateException("VirtualTimeScheduler is shutdown");
		}
		return new VirtualTimeWorker();
	}

	@Override
	public long now(TimeUnit unit) {
		return unit.convert(nanoTime + deferredNanoTime, TimeUnit.NANOSECONDS);
	}

	@Override
	public Disposable schedule(Runnable task) {
		if (shutdown) {
			throw Exceptions.failWithRejected();
		}
		return directWorker.schedule(task);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		if (shutdown) {
			throw Exceptions.failWithRejected();
		}
		return directWorker.schedule(task, delay, unit);
	}

	@Override
	public boolean isDisposed() {
		return shutdown;
	}

	@Override
	public void dispose() {
		if (shutdown) {
			return;
		}
		queue.clear();
		shutdown = true;
		directWorker.dispose();
		//TODO remove the below behavior?
		VirtualTimeScheduler s = CURRENT.get();
		if (s != null && s == this && CURRENT.compareAndSet(s, null)) {
			Schedulers.resetFactory();
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period, TimeUnit unit) {
		if (shutdown) {
			throw Exceptions.failWithRejected();
		}

		PeriodicDirectTask periodicTask = new PeriodicDirectTask(task);

		directWorker.schedulePeriodically(periodicTask, initialDelay, period, unit);

		return periodicTask;
	}

	final void advanceTime(long timeShiftInNanoseconds) {
		Operators.addCap(DEFERRED_NANO_TIME, this, timeShiftInNanoseconds);
		drain();
	}

	final void drain() {
		int remainingWork = ADVANCE_TIME_WIP.incrementAndGet(this);
		if (remainingWork != 1) {
			return;
		}
		for(;;) {
			if (!defer || !queue.isEmpty()) {
				//resetting for the first time a delayed schedule is called after a deferredNanoTime is set
				long targetNanoTime = nanoTime + DEFERRED_NANO_TIME.getAndSet(this, 0);

				while (!queue.isEmpty()) {
					TimedRunnable current = queue.peek();
					if (current == null || current.time > targetNanoTime) {
						break;
					}
					//for the benefit of tasks that call `now()`
					// if scheduled time is 0 (immediate) use current virtual time
					nanoTime = current.time == 0 ? nanoTime : current.time;
					queue.remove();

					// Only execute if not unsubscribed
					if (!current.scheduler.shutdown) {
						current.run.run();
					}
				}
				nanoTime = targetNanoTime;
			}

			remainingWork = ADVANCE_TIME_WIP.addAndGet(this, -remainingWork);
			if (remainingWork == 0) {
				break;
			}
		}
	}

	static final class TimedRunnable implements Comparable<TimedRunnable> {

		final long              time;
		final Runnable          run;
		final VirtualTimeWorker scheduler;
		final long              count; // for differentiating tasks at same time

		TimedRunnable(VirtualTimeWorker scheduler, long time, Runnable run, long count) {
			this.time = time;
			this.run = run;
			this.scheduler = scheduler;
			this.count = count;
		}

		@Override
		public int compareTo(TimedRunnable o) {
			if (time == o.time) {
				return compare(count, o.count);
			}
			return compare(time, o.time);
		}

		static int compare(long a, long b){
			return a < b ? -1 : (a > b ? 1 : 0);
		}
	}

	static final class AllFactory implements Schedulers.Factory {

		final VirtualTimeScheduler s;

		AllFactory(VirtualTimeScheduler s) {
			this.s = s;
		}

		@Override
		public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
			return s;
		}

		@Override
		public Scheduler newBoundedElastic(int threadCap, int taskCap, ThreadFactory threadFactory, int ttlSeconds) {
			return s;
		}

		@Override
		public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
			return s;
		}

		@Override
		public Scheduler newSingle(ThreadFactory threadFactory) {
			return s;
		}
	}

	final class VirtualTimeWorker implements Worker {

		volatile boolean shutdown;

		VirtualTimeWorker() { }

		@Override
		public Disposable schedule(Runnable run) {
			if (shutdown) {
				throw Exceptions.failWithRejected();
			}
			final TimedRunnable timedTask = new TimedRunnable(this,
					0,
					run,
					COUNTER.getAndIncrement(VirtualTimeScheduler.this));
			queue.add(timedTask);
			drain();
			return () -> {
				queue.remove(timedTask);
				drain();
			};
		}

		@Override
		public Disposable schedule(Runnable run, long delayTime, TimeUnit unit) {
			if (shutdown) {
				throw Exceptions.failWithRejected();
			}
			final TimedRunnable timedTask = new TimedRunnable(this,
					nanoTime + unit.toNanos(delayTime),
					run,
					COUNTER.getAndIncrement(VirtualTimeScheduler.this));
			queue.add(timedTask);
			drain();
			return () -> {
				queue.remove(timedTask);
				drain();
			};
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			final long periodInNanoseconds = unit.toNanos(period);
			final long firstNowNanoseconds = nanoTime;
			final long firstStartInNanoseconds = firstNowNanoseconds + unit.toNanos(initialDelay);

			PeriodicTask periodicTask = new PeriodicTask(firstStartInNanoseconds, task,
					firstNowNanoseconds,
					periodInNanoseconds);

			replace(periodicTask, schedule(periodicTask, initialDelay, unit));
			return periodicTask;
		}

		@Override
		public void dispose() {
			shutdown = true;
		}

		@Override
		public boolean isDisposed() {
			return shutdown;
		}

		final class PeriodicTask extends AtomicReference<Disposable> implements Runnable,
		                                                                        Disposable {

			final Runnable decoratedRun;
			final long     periodInNanoseconds;
			long count;
			long lastNowNanoseconds;
			long startInNanoseconds;

			PeriodicTask(long firstStartInNanoseconds,
					Runnable decoratedRun,
					long firstNowNanoseconds,
					long periodInNanoseconds) {
				this.decoratedRun = decoratedRun;
				this.periodInNanoseconds = periodInNanoseconds;
				lastNowNanoseconds = firstNowNanoseconds;
				startInNanoseconds = firstStartInNanoseconds;
				lazySet(EMPTY);
			}

			@Override
			public void run() {
				decoratedRun.run();

				if (get() != CANCELLED && !shutdown) {

					long nextTick;

					long nowNanoseconds = nanoTime;
					// If the clock moved in a direction quite a bit, rebase the repetition period
					if (nowNanoseconds + CLOCK_DRIFT_TOLERANCE_NANOSECONDS < lastNowNanoseconds || nowNanoseconds >= lastNowNanoseconds + periodInNanoseconds + CLOCK_DRIFT_TOLERANCE_NANOSECONDS) {
						nextTick = nowNanoseconds + periodInNanoseconds;
		                /*
                         * Shift the start point back by the drift as if the whole thing
                         * started count periods ago.
                         */
						startInNanoseconds = nextTick - (periodInNanoseconds * (++count));
					}
					else {
						nextTick = startInNanoseconds + (++count * periodInNanoseconds);
					}
					lastNowNanoseconds = nowNanoseconds;

					long delay = nextTick - nowNanoseconds;
					replace(this, schedule(this, delay, TimeUnit.NANOSECONDS));
				}
			}

			@Override
			public void dispose() {
				getAndSet(CANCELLED).dispose();
			}
		}
	}

	static final Disposable CANCELLED = Disposables.disposed();
	static final Disposable EMPTY = Disposables.never();

	static boolean replace(AtomicReference<Disposable> ref, @Nullable Disposable c) {
		for (; ; ) {
			Disposable current = ref.get();
			if (current == CANCELLED) {
				if (c != null) {
					c.dispose();
				}
				return false;
			}
			if (ref.compareAndSet(current, c)) {
				return true;
			}
		}
	}

	static class PeriodicDirectTask implements Runnable, Disposable {

		final Runnable run;

		volatile boolean disposed;

		PeriodicDirectTask(Runnable run) {
			this.run = run;
		}

		@Override
		public void run() {
			if (!disposed) {
				try {
					run.run();
				}
				catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					throw Exceptions.propagate(ex);
				}
			}
		}

		@Override
		public void dispose() {
			disposed = true;
		}
	}

	static final AtomicReference<VirtualTimeScheduler> CURRENT = new AtomicReference<>();

	static final AtomicLongFieldUpdater<VirtualTimeScheduler> COUNTER =
			AtomicLongFieldUpdater.newUpdater(VirtualTimeScheduler.class, "counter");
	static final long CLOCK_DRIFT_TOLERANCE_NANOSECONDS;

	static {
		CLOCK_DRIFT_TOLERANCE_NANOSECONDS = TimeUnit.MINUTES.toNanos(Long.getLong(
				"reactor.scheduler.drift-tolerance",
				15));
	}


}

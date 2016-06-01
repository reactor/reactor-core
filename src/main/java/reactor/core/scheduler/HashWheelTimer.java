/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongSupplier;

import reactor.core.flow.*;
import reactor.core.queue.RingBuffer;
import reactor.core.state.*;
import reactor.core.util.*;

/**
 * Hash Wheel HashWheelTimer, as per the paper: <p> Hashed and hierarchical timing wheels: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 * <p> More comprehensive slides, explaining the paper can be found here: http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 * <p> Hash Wheel timer is an approximated timer that allows performant execution of larger amount of tasks with better
 * performance compared to traditional scheduling.
 *
 * @author Oleksandr Petrov
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.0, 2.0, 2.5
 */
final class HashWheelTimer implements Introspectable, Cancellable, TimedScheduler {

	final ThreadWorker loop;
	final Executor     executor;
	final AtomicBoolean started = new AtomicBoolean();

	@SuppressWarnings("unused")
	volatile long subscriptions;

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code res} and {@code wheelSize}. All times will rounded up to
	 * the closest multiple of this resolution.
	 *
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the HashWheelTimer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param waitStrategy strategy for waiting for the next tick
	 */
	HashWheelTimer(int res, int wheelSize, WaitStrategy waitStrategy) {
		this(DEFAULT_TIMER_NAME, res, wheelSize, waitStrategy, null, SYSTEM_NOW);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code wheelSize}. All times will rounded
	 * up to the closest multiple of this resolution.
	 *
	 * @param name name for daemon thread factory to be displayed
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the HashWheelTimer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param strategy strategy for waiting for the next tick
	 * @param exec {@code Executor} instance to submit tasks to
	 */
	HashWheelTimer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec) {
		this(name, res, wheelSize, strategy, exec, SYSTEM_NOW);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code wheelSize}. All times will rounded
	 * up to the closest multiple of this resolution.
	 *
	 * @param name name for daemon thread factory to be displayed
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the HashWheelTimer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param strategy strategy for waiting for the next tick
	 * @param exec {@code Executor} instance to submit tasks to
	 * @param timeResolver {@code LongSupplier} supplier returning current time in milliseconds every time it's called
	 */
	HashWheelTimer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec, LongSupplier timeResolver) {
		if (exec == null) {
			this.executor = Executors.newFixedThreadPool(1,
					r -> {
						Thread t = new Thread(r, name + "-run");
						t.setContextClassLoader(new ClassLoader(Thread.currentThread()
						                                              .getContextClassLoader()) {
						});
						return t;
					});
		}
		else {
			this.executor = exec;
		}

		this.loop = new ThreadWorker(name, res, timeResolver, wheelSize, strategy, this);
	}

	@Override
	public boolean isCancelled() {
		return loop.isInterrupted();
	}

	@Override
	public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
		return loop.schedule(task, delay, unit);
	}

	@Override
	public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		return loop.schedulePeriodically(task, initialDelay, period, unit);
	}

	@Override
	public TimedWorker createWorker() {
		return new CompositeTimedWorker(loop);
	}

	@Override
	public Cancellation schedule(Runnable task) {
		return loop.schedule(task);
	}

	@Override
	public long getPeriod() {
		return loop.resolution;
	}

	/**
	 * Cancel current HashWheelTimer
	 */
	@Override
	public void shutdown() {
		this.loop.interrupt();
		if(executor instanceof ExecutorService){
			((ExecutorService)executor).shutdown();
		}
	}

	/**
	 * Start the HashWheelTimer, may throw an IllegalStateException if already started.
	 */
	@Override
	public void start() {
		if (started.compareAndSet(false, true)) {
			this.loop.start();
			loop.wheel.publish(0);
		}
		else {
			throw new IllegalStateException("HashWheelTimer already started");
		}
	}

	@Override
	public String toString() {
		return loop.toString();
	}

	static void checkResolution(long time, long resolution) {
		if (time % resolution != 0) {
			throw Exceptions.bubble(new IllegalArgumentException(
					"Period must be a multiple of HashWheelTimer resolution (e.g. period % resolution == 0 ). " +
							"Resolution for this HashWheelTimer is: " + resolution + "ms"));
		}
	}

	final static class ThreadWorker extends Thread implements TimedWorker {

		final RingBuffer<Set<HashWheelTask>> wheel;
		final HashWheelTimer                 parent;
		final LongSupplier                   timeMillisResolver;
		final WaitStrategy                   waitStrategy;
		final int                            resolution;

		ThreadWorker(String name,
				int resolution,
				LongSupplier timeMillisResolver,
				int wheelSize,
				WaitStrategy waitStrategy,
				HashWheelTimer parent) {
			super(name);
			this.parent = parent;
			this.resolution = resolution;
			this.wheel = RingBuffer.createSingleProducer(ConcurrentSkipListSet::new, wheelSize);
			this.timeMillisResolver = timeMillisResolver;
			this.waitStrategy = waitStrategy;
		}

		@Override
		public void run() {
			long deadline = timeMillisResolver.getAsLong();

			Runnable noop = () -> {
				if (isInterrupted()) {
					throw Exceptions.AlertException.INSTANCE;
				}
			};

			for (; ; ) {
				Set<HashWheelTask> registrations = wheel.get(wheel.getCursor());

				for (HashWheelTask r : registrations) {
					if (r.isCancelled()) {
						registrations.remove(r);
						if (SUBSCRIPTIONS.decrementAndGet(parent) == 0) {
							shutdown();
						}
					}
					else if (r.ready()) {
						try {
							parent.executor.execute(r);
						}
						catch (RejectedExecutionException re) {
							if (isInterrupted()) {
								break;
							}
							throw re;
						}

						registrations.remove(r);

						if (r.asInterval() != null) {
							reschedule(r.asInterval(), r.asInterval().rescheduleRounds);
						}
					}
					else {
						r.decrement();
					}
				}

				deadline += resolution;

				try {
					waitStrategy.waitFor(deadline, timeMillisResolver, noop);
				}
				catch (Exceptions.AlertException | InterruptedException e) {
					break;
				}

				wheel.publish(wheel.next());
			}

			if (parent.executor instanceof Scheduler) {
				((Scheduler) parent.executor).shutdown();
			}
			else if (parent.executor instanceof ExecutorService) {
				((ExecutorService) parent.executor).shutdown();
			}
		}

		@Override
		public Cancellation schedule(Runnable task) {
			return schedule(task, resolution, TimeUnit.MILLISECONDS);
		}

		@Override
		public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
			if (isInterrupted() || !isAlive()) {
				throw Exceptions.failWithCancel();
			}
			long delayMs = TimeUnit.MILLISECONDS.convert(delay, unit);

			if (delay != 0) {
				checkResolution(delayMs, resolution);
			}

			long firstFireOffset = delayMs / resolution;
			long firstFireRounds = firstFireOffset / wheel.getCapacity();

			HashWheelTask r =
					new TimerTask(resolution * wheel.getCapacity(), firstFireRounds, 0L, task);

			wheel.get(wheel.getCursor() + firstFireOffset)
			     .add(r);

			return r;
		}

		@Override
		public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			if (isInterrupted() || !isAlive()) {
				throw Exceptions.failWithCancel();
			}
			long periodMs = TimeUnit.MILLISECONDS.convert(period, unit);
			long initialDelayMs = TimeUnit.MILLISECONDS.convert(initialDelay, unit);

			if (initialDelay != 0) {
				checkResolution(initialDelayMs, resolution);
			}
			checkResolution(periodMs, resolution);

			long offset = periodMs / resolution;
			long rounds = offset / wheel.getCapacity();

			long firstFireOffset = initialDelayMs / resolution;
			long firstFireRounds = firstFireOffset / wheel.getCapacity();

			HashWheelTask r = new IntervalTask(resolution * wheel.getCapacity(),
					firstFireRounds,
					offset,
					task,
					rounds);

			wheel.get(wheel.getCursor() + firstFireOffset + (period != 0 ? 1 : 0))
			     .add(r);

			SUBSCRIPTIONS.incrementAndGet(parent);

			return r;
		}

		@Override
		public void shutdown() {
		}

		@Override
		public String toString() {
			return String.format("HashWheelTimer { Buffer Size: %d, Resolution: %d }", wheel.getCapacity(), resolution);
		}

		/**
		 * Reschedule a {@link IntervalTask}  for the next fire
		 */
		void reschedule(HashWheelTask registration, long rounds) {
			HashWheelTask.ROUNDS.set(registration, rounds);
			wheel.get(wheel.getCursor() + registration.scheduleOffset)
			     .add(registration);
		}

	}

	static abstract class HashWheelTask
			implements Runnable, Cancellation, Comparable, Cancellable, Producer, Introspectable {

		final    Runnable task;
		final    long     scheduleOffset;
		final    long     wheelResolution;
		volatile long     rounds;
		volatile int      status;

		HashWheelTask(long wheelResolution,
				Runnable task,
				long rounds,
				long scheduleOffset) {
			this.wheelResolution = wheelResolution;
			this.task = Objects.requireNonNull(task, "Must provide a task");
			this.scheduleOffset = scheduleOffset;
			ROUNDS.lazySet(this, rounds);
			STATUS.lazySet(this, STATUS_READY);
		}

		@Override
		public void dispose() {
			STATUS.set(this, STATUS_CANCELLED);
		}

		@Override
		public final int compareTo(Object o) {
			HashWheelTask other = (HashWheelTask) o;
			if (rounds == other.rounds) {
				return other == this ? 0 : -1;
			}
			else {
				return Long.compare(rounds, other.rounds);
			}
		}

		@Override
		public final Object downstream() {
			return task;
		}

		/**
		 * Check whether the current Registration is cancelled
		 *
		 * @return whether or not the current Registration is cancelled
		 */
		@Override
		public final boolean isCancelled() {
			return status == STATUS_CANCELLED;
		}

		@Override
		public final long getPeriod() {
			return rounds * wheelResolution;
		}

		@Override
		public final String toString() {
			return String.format("HashWheelTimer { Rounds left: %d, Status: %d }", rounds, status);
		}

		abstract IntervalTask asInterval();

		/**
		 * Decrement an amount of runs Registration has to run until it's elapsed
		 */
		final void decrement() {
			ROUNDS.decrementAndGet(this);
		}

		/**
		 * Check whether the current Registration is ready for execution
		 *
		 * @return whether or not the current Registration is ready for execution
		 */
		boolean ready() {
			return status == STATUS_READY && rounds == 0;
		}

		static final AtomicLongFieldUpdater<HashWheelTask>    ROUNDS =
				AtomicLongFieldUpdater.newUpdater(HashWheelTask.class, "rounds");
		static final AtomicIntegerFieldUpdater<HashWheelTask> STATUS =
				AtomicIntegerFieldUpdater.newUpdater(HashWheelTask.class, "status");
	}

	static final class IntervalTask extends HashWheelTask {

		final long rescheduleRounds;

		IntervalTask(long resolution,
				long rounds,
				long offset,
				Runnable task,
				long rescheduleRounds) {
			super(resolution, task, rounds, offset);
			this.rescheduleRounds = rescheduleRounds;
		}

		@Override
		public void run() {
			if (isCancelled()) {
				return;
			}
			task.run();
		}

		@Override
		IntervalTask asInterval() {
			return this;
		}

	}

	final static class TimerTask extends HashWheelTask {

		TimerTask(long resolution, long rounds, long offset, Runnable task) {
			super(resolution, task, rounds, offset);
		}

		@Override
		public IntervalTask asInterval() {
			return null;
		}

		@Override
		public void run() {
			if (STATUS.compareAndSet(this, STATUS_READY, STATUS_EMITTED)) {
				task.run();
			}
		}

		@Override
		boolean ready() {
			return rounds == 0L && status == STATUS_READY;
		}

		final static int STATUS_EMITTED   = 2;
	}

	static final String                                 DEFAULT_TIMER_NAME = "hash-wheel-timer";
	static final int                                    STATUS_CANCELLED   = -1;
	static final int                                    STATUS_READY       = 0;
	static final AtomicLongFieldUpdater<HashWheelTimer> SUBSCRIPTIONS      =
			AtomicLongFieldUpdater.newUpdater(HashWheelTimer.class, "subscriptions");

	final static LongSupplier SYSTEM_NOW = System::currentTimeMillis;
}

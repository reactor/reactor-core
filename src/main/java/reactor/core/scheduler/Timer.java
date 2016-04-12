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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;

import reactor.core.flow.Cancellation;
import reactor.core.flow.Producer;
import reactor.core.queue.RingBuffer;
import reactor.core.state.Cancellable;
import reactor.core.state.Introspectable;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.WaitStrategy;

/**
 * Hash Wheel Timer, as per the paper: <p> Hashed and hierarchical timing wheels: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 * <p> More comprehensive slides, explaining the paper can be found here: http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 * <p> Hash Wheel timer is an approximated timer that allows performant execution of larger amount of tasks with better
 * performance compared to traditional scheduling.
 *
 * @author Oleksandr Petrov
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.0, 2.0, 2.5
 */
public class Timer implements Introspectable, Cancellable, TimedScheduler {

	public static final int DEFAULT_WHEEL_SIZE = 512;

	/**
	 * Create a new {@link Timer} using the default resolution (50MS) and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 * <p>
	 * return a new {@link Timer}
	 */
	public static Timer create() {
		return create(50);
	}

	/**
	 * Create a new {@link Timer} using the default resolution (50MS) and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 * @param name timer thread prefix
	 * <p>
	 * return a new {@link Timer}
	 */
	public static Timer create(String name) {
		return create(name, 50);
	}

	/**
	 * Create a new {@link Timer} using the the given timer {@code resolution} and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param resolution resolution of this timer in milliseconds
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(int resolution) {
		return create(resolution, 64);
	}

	/**
	 * Create a new {@link Timer} using the the given timer {@code resolution} and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(String name, int resolution) {
		return create(name, resolution, 64);
	}

	/**
	 * Create a new {@code Timer} using the given timer {@code resolution} and {@code bufferSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param resolution resolution of this timer in milliseconds
	 * @param bufferSize size of the wheel supporting the Timer, the larger the wheel, the less the lookup time is
	 *                   for sparse timeouts.
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(int resolution, int bufferSize) {
		return create("reactor-timer", resolution, bufferSize);
	}

	/**
	 * Create a new {@code Timer} using the given timer {@code resolution} and {@code bufferSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 * @param bufferSize size of the wheel supporting the Timer, the larger the wheel, the less the lookup time is
	 *                   for sparse timeouts.
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(String name, int resolution, int bufferSize) {
		Timer timer = new Timer(name, resolution, bufferSize, WaitStrategy.sleeping(), null);
		timer.start();
		return timer;
	}

	/**
	 * Obtain the default global timer from the current context. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The global timer will also ignore {@link Timer#shutdown()} calls and should be cleaned using {@link
	 * #unregisterGlobal()}.
	 * <p>
	 * The default globalTimer is a {@link Timer}. It is suitable for non blocking periodic
	 * work
	 * such as  eventing, memory access, lock-free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link Timer}
	 */
	public static Timer global() {
		return GlobalTimer.get();
	}

	/**
	 * The returned timer SHOULD always be cancelled after use, however global timer will ignore it.
	 *
	 * @return eventually the global timer or if not set a fresh timer.
	 */
	public static Timer globalOrNew() {
		return GlobalTimer.globalOrNew();
	}

	/**
	 * The returned timer MIGHT be NULL if no global timer set.
	 *
	 * @return eventually the global timer or if not NULL.
	 */
	public static Timer globalOrNull() {
		return GlobalTimer.available() ? GlobalTimer.get() : null;
	}

	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	public static boolean hasGlobal() {
		return GlobalTimer.available();
	}

	/**
	 * Clean current global timer references and cancel the respective {@link Timer}.
	 * A new global timer can be assigned later with {@link #global()}.
	 */
	public static void unregisterGlobal() {
		GlobalTimer.unregister();
	}

	final ThreadWorker loop;
	final Executor     executor;
	final AtomicBoolean started = new AtomicBoolean();

	@SuppressWarnings("unused")
	volatile long subscriptions;

	/**
	 * Create a new {@code Timer} using the given timer {@code res} and {@code wheelSize}. All times will rounded up to
	 * the closest multiple of this resolution.
	 *
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param waitStrategy strategy for waiting for the next tick
	 */
	Timer(int res, int wheelSize, WaitStrategy waitStrategy) {
		this(DEFAULT_TIMER_NAME, res, wheelSize, waitStrategy, null, SYSTEM_NOW);
	}

	/**
	 * Create a new {@code Timer} using the given timer {@code resolution} and {@code wheelSize}. All times will rounded
	 * up to the closest multiple of this resolution.
	 *
	 * @param name name for daemon thread factory to be displayed
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param strategy strategy for waiting for the next tick
	 * @param exec {@code Executor} instance to submit tasks to
	 */
	Timer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec) {
		this(name, res, wheelSize, strategy, exec, SYSTEM_NOW);
	}

	/**
	 * Create a new {@code Timer} using the given timer {@code resolution} and {@code wheelSize}. All times will rounded
	 * up to the closest multiple of this resolution.
	 *
	 * @param name name for daemon thread factory to be displayed
	 * @param res resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 * for sparse timeouts. Sane default is 512.
	 * @param strategy strategy for waiting for the next tick
	 * @param exec {@code Executor} instance to submit tasks to
	 * @param timeResolver {@code LongSupplier} supplier returning current time in milliseconds every time it's called
	 */
	Timer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec, LongSupplier timeResolver) {
		if (exec == null) {
			this.executor = Executors.newFixedThreadPool(1,
					ExecutorUtils.newNamedFactory(name + "-run",
							new ClassLoader(Thread.currentThread()
							                      .getContextClassLoader()) {
							}));
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
		return null;
	}

	@Override
	public TimedWorker createWorker() {
		return loop;
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
	 * Cancel current Timer
	 */
	@Override
	public void shutdown() {
		this.loop.interrupt();
	}

	/**
	 * Start the Timer, may throw an IllegalStateException if already started.
	 */
	@Override
	public void start() {
		if (started.compareAndSet(false, true)) {
			this.loop.start();
			loop.wheel.publish(0);
		}
		else {
			throw new IllegalStateException("Timer already started");
		}
	}

	@Override
	public String toString() {
		return loop.toString();
	}

	static void checkResolution(long time, long resolution) {
		if (time % resolution != 0) {
			throw Exceptions.bubble(new IllegalArgumentException(
					"Period must be a multiple of Timer resolution (e.g. period % resolution == 0 ). " +
							"Resolution for this Timer is: " + resolution + "ms"));
		}
	}

	final static class ThreadWorker extends Thread implements TimedWorker {

		final RingBuffer<Set<Timer.HashWheelSubscription>> wheel;
		final Timer                                        parent;
		final LongSupplier                                 timeMillisResolver;
		final WaitStrategy                                 waitStrategy;
		final int                                          resolution;

		ThreadWorker(String name,
				int resolution,
				LongSupplier timeMillisResolver,
				int wheelSize,
				WaitStrategy waitStrategy,
				Timer parent) {
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
				Set<Timer.HashWheelSubscription> registrations = wheel.get(wheel.getCursor());

				for (Timer.HashWheelSubscription r : registrations) {
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
		public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
			if (isInterrupted() || !isAlive()) {
				throw Exceptions.failWithCancel();
			}
			long delayMs = unit.convert(delay, TimeUnit.MILLISECONDS);

			if (delay != 0) {
				checkResolution(delayMs, resolution);
			}

			long firstFireOffset = delayMs / resolution;
			long firstFireRounds = firstFireOffset / wheel.getCapacity();

			Timer.HashWheelSubscription r =
					new Timer.TimerSubscription(resolution * wheel.getCapacity(), firstFireRounds, 0L, task);

			wheel.get(wheel.getCursor() + firstFireOffset)
			     .add(r);

			return r;
		}

		@Override
		public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			if (isInterrupted() || !isAlive()) {
				throw Exceptions.failWithCancel();
			}
			long periodMs = unit.convert(period, TimeUnit.MILLISECONDS);
			long initialDelayMs = unit.convert(initialDelay, TimeUnit.MILLISECONDS);

			if (initialDelay != 0) {
				checkResolution(initialDelayMs, resolution);
			}
			checkResolution(periodMs, resolution);

			long offset = periodMs / resolution;
			long rounds = offset / wheel.getCapacity();

			long firstFireOffset = initialDelayMs / resolution;
			long firstFireRounds = firstFireOffset / wheel.getCapacity();

			Timer.HashWheelSubscription r = new Timer.IntervalSubscription(resolution * wheel.getCapacity(),
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
		public Cancellation schedule(Runnable task) {
			return schedule(task, resolution, TimeUnit.MILLISECONDS);
		}

		@Override
		public void shutdown() {
		}

		@Override
		public String toString() {
			return String.format("Timer { Buffer Size: %d, Resolution: %d }", wheel.getCapacity(), resolution);
		}

		/**
		 * Reschedule a {@link Timer.IntervalSubscription}  for the next fire
		 */
		void reschedule(Timer.HashWheelSubscription registration, long rounds) {
			Timer.HashWheelSubscription.ROUNDS.set(registration, rounds);
			wheel.get(wheel.getCursor() + registration.scheduleOffset)
			     .add(registration);
		}

	}

	static abstract class HashWheelSubscription
			implements Runnable, Cancellation, Comparable, Cancellable, Producer, Introspectable {

		final    Runnable task;
		final    long     scheduleOffset;
		final    long     wheelResolution;
		volatile long     rounds;
		volatile int      status;

		HashWheelSubscription(long wheelResolution,
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
			Timer.HashWheelSubscription other = (Timer.HashWheelSubscription) o;
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
			return String.format("Timer { Rounds left: %d, Status: %d }", rounds, status);
		}

		abstract Timer.IntervalSubscription asInterval();

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

		static final AtomicLongFieldUpdater<Timer.HashWheelSubscription>    ROUNDS =
				AtomicLongFieldUpdater.newUpdater(Timer.HashWheelSubscription.class, "rounds");
		static final AtomicIntegerFieldUpdater<Timer.HashWheelSubscription> STATUS =
				AtomicIntegerFieldUpdater.newUpdater(Timer.HashWheelSubscription.class, "status");
	}

	static final class IntervalSubscription extends Timer.HashWheelSubscription {

		final long rescheduleRounds;

		IntervalSubscription(long resolution,
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
		Timer.IntervalSubscription asInterval() {
			return this;
		}

	}

	final static class TimerSubscription extends Timer.HashWheelSubscription {

		TimerSubscription(long resolution, long rounds, long offset, Runnable task) {
			super(resolution, task, rounds, offset);
		}

		@Override
		public Timer.IntervalSubscription asInterval() {
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

	static final String                        DEFAULT_TIMER_NAME = "hash-wheel-timer";
	static final int                           STATUS_CANCELLED   = -1;
	static final int                           STATUS_READY       = 0;
	static final AtomicLongFieldUpdater<Timer> SUBSCRIPTIONS      =
			AtomicLongFieldUpdater.newUpdater(Timer.class, "subscriptions");

	final static LongSupplier SYSTEM_NOW = System::currentTimeMillis;
}

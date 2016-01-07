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

package reactor.core.timer;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Processor;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.WaitStrategy;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.fn.Consumer;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * Hash Wheel Timer, as per the paper: <p> Hashed and hierarchical timing wheels: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 * <p> More comprehensive slides, explaining the paper can be found here: http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 * <p> Hash Wheel timer is an approximated timer that allows performant execution of larger amount of tasks with better
 * performance compared to traditional scheduling.
 * @author Oleksandr Petrov
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class HashWheelTimer extends Timer {

	public static final  int    DEFAULT_WHEEL_SIZE = 512;
	private static final String DEFAULT_TIMER_NAME = "hash-wheel-timer";

	private final RingBuffer<Set<TimedSubscription>> wheel;
	private final Thread                             loop;
	private final Executor                           executor;
	private final WaitStrategy                       waitStrategy;
	private final LongSupplier                       timeMillisResolver;
	private final AtomicBoolean started = new AtomicBoolean();

	/**
	 * Create a new {@code HashWheelTimer} using the given timer resolution. All times will rounded up to the closest
	 * multiple of this resolution.
	 * @param resolution         the resolution of this timer, in milliseconds
	 */
	public HashWheelTimer(int resolution) {
		this(resolution, DEFAULT_WHEEL_SIZE, new WaitStrategy.Sleeping());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code res} and {@code wheelSize}. All times will
	 * rounded up to the closest multiple of this resolution.
	 * @param res                resolution of this timer in milliseconds
	 * @param wheelSize          size of the Ring Buffer supporting the Timer, the larger the wheel, the less the
	 *                           lookup time is for sparse timeouts. Sane default is 512.
	 * @param waitStrategy       strategy for waiting for the next tick
	 */
	public HashWheelTimer(int res, int wheelSize, WaitStrategy waitStrategy) {
		this(DEFAULT_TIMER_NAME, res, wheelSize, waitStrategy, null, TimeUtils.currentTimeMillisResolver());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code wheelSize}. All times
	 * will rounded up to the closest multiple of this resolution.
	 * @param name               name for daemon thread factory to be displayed
	 * @param res                resolution of this timer in milliseconds
	 * @param wheelSize          size of the Ring Buffer supporting the Timer, the larger the wheel, the less the
	 *                           lookup time is for sparse timeouts. Sane default is 512.
	 * @param strategy           strategy for waiting for the next tick
	 * @param exec               {@code Executor} instance to submit tasks to
	 */
	public HashWheelTimer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec) {
		this(name, res,wheelSize, strategy, exec, TimeUtils.currentTimeMillisResolver());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code wheelSize}. All times
	 * will rounded up to the closest multiple of this resolution.
	 * @param name               name for daemon thread factory to be displayed
	 * @param res                resolution of this timer in milliseconds
	 * @param wheelSize          size of the Ring Buffer supporting the Timer, the larger the wheel, the less the
	 *                           lookup time is for sparse timeouts. Sane default is 512.
	 * @param strategy           strategy for waiting for the next tick
	 * @param exec               {@code Executor} instance to submit tasks to
	 * @param timeResolver       {@code LongSupplier} supplier returning current time in milliseconds every time it's
	 *                           called
	 */
	public HashWheelTimer(String name,
						  int res,
						  int wheelSize,
						  WaitStrategy strategy,
						  Executor exec,
						  LongSupplier timeResolver) {
		super(res);

		this.timeMillisResolver = timeResolver;
		this.waitStrategy = strategy;

		this.wheel = RingBuffer.createSingleProducer(new Supplier<Set<TimedSubscription>>() {
			@Override
			public Set<TimedSubscription> get() {
				return new ConcurrentSkipListSet<>();
			}
		}, wheelSize);

		if (exec == null) {
			this.executor =
					Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory(name + "-run", new ClassLoader(Thread.currentThread()
					                                                                                                  .getContextClassLoader()) {
					}));
		}
		else {
			this.executor = exec;
		}

		this.loop = new NamedDaemonThreadFactory(name).newThread(new Runnable() {
			@Override
			public void run() {
				long deadline = timeMillisResolver.get();

				Runnable noop = new Runnable() {
					@Override
					public void run() {
						if (loop.isInterrupted()) {
							throw AlertException.INSTANCE;
						}
					}
				};

				while (true) {
					Set<TimedSubscription> registrations = wheel.get(wheel.getCursor());

					for (TimedSubscription r : registrations) {
						if (r.isCancelled()) {
							registrations.remove(r);
						}
						else if (r.ready()) {
							try {
								executor.execute(r);
							}
							catch (RejectedExecutionException re) {
								if (loop.isInterrupted()) {
									break;
								}
								throw re;
							}
							registrations.remove(r);

							if (!r.isCancelAfterUse()) {
								reschedule(r);
							}
						}
						else if (r.isPaused()) {
							reschedule(r);
						}
						else {
							r.decrement();
						}
					}

					deadline += resolution;

					try {
						waitStrategy.waitFor(deadline, timeMillisResolver, noop);
					}
					catch (AlertException | InterruptedException e) {
						break;
					}

					wheel.publish(wheel.next());
				}
				if (executor instanceof Processor) {
					((Processor) executor).onComplete();
				}
				else if (executor instanceof ExecutorService) {
					((ExecutorService) executor).shutdown();
				}
			}
		});
	}

	@Override
	public long period() {
		return resolution;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Pausable submit(Consumer<Long> consumer, long period, TimeUnit timeUnit) {
		long ms = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		return schedule(0, ms, consumer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Pausable schedule(Consumer<Long> consumer, long period, TimeUnit timeUnit, long delayInMilliseconds) {
		return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), delayInMilliseconds, consumer);
	}

	@SuppressWarnings("unchecked")
	private TimedSubscription schedule(long recurringTimeout, long firstDelay, Consumer<Long> consumer) {
		if (loop.isInterrupted() || !loop.isAlive()) {
			throw CancelException.get();
		}
		if (recurringTimeout != 0) {
			TimeUtils.checkResolution(recurringTimeout, resolution);
		}

		long offset = recurringTimeout / resolution;
		long rounds = offset / wheel.getBufferSize();

		long firstFireOffset = firstDelay / resolution;
		long firstFireRounds = firstFireOffset / wheel.getBufferSize();

		TimedSubscription r;
		if (recurringTimeout != 0) {
			r = new TimedSubscription(firstFireRounds, offset, consumer, rounds, timeMillisResolver);
		}
		else{
			r = new SingleTimedSubscription<>(firstFireRounds, offset, consumer, rounds, timeMillisResolver);
		}

		wheel.get(wheel.getCursor() + firstFireOffset + (recurringTimeout != 0 ? 1 : 0))
		     .add(r);
		return r;
	}

	@Override
	public boolean isCancelled() {
		return loop.isInterrupted();
	}

	/**
	 * Reschedule a {@link TimedSubscription}  for the next fire
	 */
	private void reschedule(TimedSubscription registration) {
		registration.rounds.set(registration.rescheduleRounds);
		wheel.get(wheel.getCursor() + registration.getOffset())
		     .add(registration);
	}

	/**
	 * Start the Timer
	 */
	@Override
	public void start() {
		if (started.compareAndSet(false, true)) {
			this.loop.start();
			wheel.publish(0);
		}
		else {
			throw new IllegalStateException("Timer already started");
		}
	}

	/**
	 * Cancel current Timer
	 */
	public void cancel() {
		this.loop.interrupt();
	}

	@Override
	public String toString() {
		return String.format("HashWheelTimer { Buffer Size: %d, Resolution: %d }", wheel.getBufferSize(), resolution);
	}

	/**
	 * Timer Registration
	 * @param <T> type of the Timer Registration Consumer
	 */
	public static class TimedSubscription<T extends Consumer<Long>> implements Runnable, Comparable, Pausable {

		public static final int STATUS_PAUSED    = 1;
		public static final int STATUS_CANCELLED = -1;
		public static final int STATUS_READY     = 0;

		private final T             delegate;
		private final long          rescheduleRounds;
		private final long          scheduleOffset;
		private final AtomicLong    rounds;
		private final AtomicInteger status;
		private final boolean       lifecycle;
		private final LongSupplier  now;

		/**
		 * Creates a new Timer Registration with given {@code rounds}, {@code offset} and {@code delegate}.
		 * @param rounds amount of rounds the Registration should go through until it's elapsed
		 * @param offset offset of in the Ring Buffer for rescheduling
		 * @param delegate delegate that will be ran whenever the timer is elapsed
		 */
		public TimedSubscription(long rounds, long offset, T delegate, long rescheduleRounds, LongSupplier timeResolver) {
			Assert.notNull(delegate, "Delegate cannot be null");
			this.now = timeResolver;
			this.rescheduleRounds = rescheduleRounds;
			this.scheduleOffset = offset;
			this.delegate = delegate;
			this.rounds = new AtomicLong(rounds);
			this.status = new AtomicInteger(STATUS_READY);
			this.lifecycle = Pausable.class.isAssignableFrom(delegate.getClass());
		}

		/**
		 * Decrement an amount of runs Registration has to run until it's elapsed
		 */
		public void decrement() {
			rounds.decrementAndGet();
		}

		/**
		 * Check whether the current Registration is ready for execution
		 * @return whether or not the current Registration is ready for execution
		 */
		public boolean ready() {
			return status.get() == STATUS_READY && rounds.get() == 0;
		}

		/**
		 * Run the delegate of the current Registration
		 */
		@Override
		public void run() {
			try {
				delegate.accept(now.get());
			}
			catch (CancelException e) {
				cancel();
			}
		}

		/**
		 * Reset the Registration
		 */
		public void reset() {
			this.status.set(STATUS_READY);
			this.rounds.set(rescheduleRounds);
		}

		/**
		 * Cancel the registration
		 * @return current Registration
		 */
		@Override
		public TimedSubscription cancel() {
			if (!isCancelled()) {
				if (lifecycle) {
					((Pausable) delegate).cancel();
				}
				this.status.set(STATUS_CANCELLED);
			}
			return this;
		}

		/**
		 * Check whether the current Registration is cancelled
		 * @return whether or not the current Registration is cancelled
		 */
		public boolean isCancelled() {
			return status.get() == STATUS_CANCELLED;
		}

		/**
		 * Pause the current Regisration
		 * @return current Registration
		 */
		@Override
		public TimedSubscription pause() {
			if (!isPaused()) {
				if (lifecycle) {
					((Pausable) delegate).pause();
				}
				this.status.set(STATUS_PAUSED);
			}
			return this;
		}

		/**
		 * Check whether the current Registration is paused
		 * @return whether or not the current Registration is paused
		 */
		public final boolean isPaused() {
			return this.status.get() == STATUS_PAUSED;
		}

		/**
		 * Resume current Registration
		 * @return current Registration
		 */
		@Override
		public final TimedSubscription resume() {
			if (isPaused()) {
				if (lifecycle) {
					((Pausable) delegate).resume();
				}
				reset();
			}
			return this;
		}

		public boolean isCancelAfterUse() {
			return false;
		}

		@Override
		public int compareTo(Object o) {
			TimedSubscription other = (TimedSubscription) o;
			if (rounds.get() == other.rounds.get()) {
				return other == this ? 0 : -1;
			}
			else {
				return Long.compare(rounds.get(), other.rounds.get());
			}
		}

		@Override
		public String toString() {
			return String.format("HashWheelTimer { Rounds left: %d, Status: %d }", rounds.get(), status.get());
		}

		public long getOffset() {
			return scheduleOffset;
		}
	}

	/**
	 *
	 * @param <T>
	 */
	public static class SingleTimedSubscription<T extends Consumer<Long>> extends TimedSubscription<T>{

		public SingleTimedSubscription(long rounds,
				long offset,
				T delegate,
				long rescheduleRounds,
				LongSupplier timeResolver) {
			super(rounds, offset, delegate, rescheduleRounds, timeResolver);
		}

		@Override
		public boolean isCancelAfterUse() {
			return true;
		}
	}
}
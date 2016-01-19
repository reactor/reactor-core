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

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.WaitStrategy;
import reactor.core.support.rb.disruptor.RingBuffer;
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

	static final String DEFAULT_TIMER_NAME = "hash-wheel-timer";
	static final int    STATUS_PAUSED      = 1;
	static final int    STATUS_CANCELLED   = -1;
	static final int    STATUS_READY       = 0;
	static final Long   TIMER_LONG         = 0L;

	private final RingBuffer<Set<HashWheelSubscription>> wheel;
	private final Thread                                 loop;
	private final Executor                               executor;
	private final WaitStrategy                           waitStrategy;
	private final LongSupplier                           timeMillisResolver;
	private final AtomicBoolean started = new AtomicBoolean();

	volatile long  subscriptions;

	static final AtomicLongFieldUpdater<HashWheelTimer>    SUBSCRIPTIONS =
			AtomicLongFieldUpdater.newUpdater(HashWheelTimer.class, "subscriptions");

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

		this.wheel = RingBuffer.createSingleProducer(new Supplier<Set<HashWheelSubscription>>() {
			@Override
			public Set<HashWheelSubscription> get() {
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
					Set<HashWheelSubscription> registrations = wheel.get(wheel.getCursor());

					for (HashWheelSubscription r : registrations) {
						if (r.isCancelled()) {
							registrations.remove(r);
							if(SUBSCRIPTIONS.decrementAndGet(HashWheelTimer.this) == 0){
								cancel();
							}
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

							if (r.asInterval() != null) {
								reschedule(r.asInterval(), r.asInterval().rescheduleRounds);
							}
						}
						else if (r.isPaused()) {
							reschedule(r, resolution);
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
	public HashWheelSubscription interval(Subscriber<? super Long> consumer,
			long period,
			TimeUnit timeUnit,
			long delayInMilliseconds) {
		long ms = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		return schedule(ms, delayInMilliseconds == -1 ? ms : delayInMilliseconds, consumer);
	}

	@Override
	public HashWheelSubscription single(Subscriber<? super Long> subscriber, long delay, TimeUnit timeUnit) {
		return schedule(0, TimeUnit.MILLISECONDS.convert(delay, timeUnit), subscriber);
	}

	@SuppressWarnings("unchecked")
	private HashWheelSubscription schedule(long recurringTimeout,
			long firstDelay,
			Subscriber<? super Long> subscriber) {
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

		HashWheelSubscription r;
		if (recurringTimeout != 0) {
			r = new IntervalSubscription(resolution * wheel.getBufferSize(),
					firstFireRounds,
					offset,
					subscriber,
					rounds);
		}
		else{
			r = new TimerSubscription(resolution * wheel.getBufferSize(), firstFireRounds, offset, subscriber);
		}

		wheel.get(wheel.getCursor() + firstFireOffset + (recurringTimeout != 0 ? 1 : 0))
		     .add(r);

		SUBSCRIPTIONS.incrementAndGet(this);

		return r;
	}

	@Override
	public boolean isCancelled() {
		return loop.isInterrupted();
	}

	/**
	 * Reschedule a {@link IntervalSubscription}  for the next fire
	 */
	void reschedule(HashWheelSubscription registration, long rounds) {
		HashWheelSubscription.ROUNDS.set(registration, rounds);
		wheel.get(wheel.getCursor() + registration.scheduleOffset)
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

	static abstract class HashWheelSubscription
			implements Runnable, Comparable, Pausable, Subscription, ActiveDownstream, Downstream, Timed {

		volatile long rounds;
		volatile int  status;

		static final AtomicLongFieldUpdater<HashWheelSubscription>    ROUNDS =
				AtomicLongFieldUpdater.newUpdater(HashWheelSubscription.class, "rounds");
		static final AtomicIntegerFieldUpdater<HashWheelSubscription> STATUS =
				AtomicIntegerFieldUpdater.newUpdater(HashWheelSubscription.class, "status");

		final Subscriber<? super Long> delegate;
		final long                     scheduleOffset;
		final long                     wheelResolution;

		HashWheelSubscription(long wheelResolution,
				Subscriber<? super Long> delegate,
				long rounds,
				long scheduleOffset) {
			this.wheelResolution = wheelResolution;
			this.delegate = Objects.requireNonNull(delegate, "Must provide a subscriber");
			this.scheduleOffset = scheduleOffset;
			ROUNDS.lazySet(this, rounds);
			STATUS.lazySet(this, STATUS_READY);
		}

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

		/**
		 * Cancel the registration
		 */
		@Override
		public final void cancel() {
			STATUS.set(this, STATUS_CANCELLED);
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

		/**
		 * Pause the current Regisration
		 */
		@Override
		public final void pause() {
			STATUS.compareAndSet(this, STATUS_READY, STATUS_PAUSED);
		}

		/**
		 * Check whether the current Registration is paused
		 *
		 * @return whether or not the current Registration is paused
		 */
		boolean isPaused() {
			return this.status == STATUS_PAUSED;
		}

		/**
		 * Resume current Registration
		 */
		@Override
		public final void resume() {
			if (STATUS.compareAndSet(this, STATUS_PAUSED, STATUS_READY)) {
				ROUNDS.set(this, scheduleOffset);
			}
		}

		abstract IntervalSubscription asInterval();

		@Override
		public final String toString() {
			return String.format("HashWheelTimer { Rounds left: %d, Status: %d }", rounds, status);
		}

		@Override
		public final int compareTo(Object o) {
			HashWheelSubscription other = (HashWheelSubscription) o;
			if (rounds == other.rounds) {
				return other == this ? 0 : -1;
			}
			else {
				return Long.compare(rounds, other.rounds);
			}
		}

		@Override
		public final Object downstream() {
			return delegate;
		}

		@Override
		public final long period() {
			return rounds * wheelResolution;
		}
	}

	static final class IntervalSubscription extends HashWheelSubscription implements DownstreamDemand {

		final long rescheduleRounds;

		long increment;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<IntervalSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IntervalSubscription.class, "requested");

		/**
		 * Creates a new Timer Registration with given {@code rounds}, {@code offset} and {@code delegate}.
		 *
		 * @param rounds amount of rounds the Registration should go through until it's elapsed
		 * @param offset offset of in the Ring Buffer for rescheduling
		 * @param delegate delegate that will be ran whenever the timer is elapsed
		 */
		public IntervalSubscription(long resolution,
				long rounds,
				long offset,
				Subscriber<? super Long> delegate,
				long rescheduleRounds) {
			super(resolution, delegate, rounds, offset);
			this.rescheduleRounds = rescheduleRounds;
		}

		@Override
		IntervalSubscription asInterval() {
			return this;
		}

		@Override
		public void run() {
			if (isCancelled() || isPaused()){
				return;
			}
			if (BackpressureUtils.getAndSub(REQUESTED, this, 1L) != 0L) {
				delegate.onNext(increment++);
			}
			else {
				cancel();
				delegate.onError(Exceptions.TimerOverflow.get());
			}
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, delegate)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}
	}

	/**
	 *
	 */
	final static class TimerSubscription extends HashWheelSubscription {

		final static int STATUS_REQUESTED = 2;
		final static int STATUS_PAUSED_REQUESTED = 3;
		final static int STATUS_EMITTED = 4;

		public TimerSubscription(long resolution, long rounds, long offset, Subscriber<? super Long> delegate) {
			super(resolution, delegate, rounds, offset);
		}

		@Override
		public IntervalSubscription asInterval() {
			return null;
		}

		@Override
		public void run() {
			if (STATUS.compareAndSet(this, STATUS_REQUESTED, STATUS_EMITTED)) {
				delegate.onNext(TIMER_LONG);
				if(STATUS.compareAndSet(this, STATUS_EMITTED, STATUS_CANCELLED)) {
					delegate.onComplete();
				}
			}
			else if (STATUS.get(this) == STATUS_READY) {
				delegate.onError(Exceptions.TimerOverflow.get());
			}
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, delegate)) {
				for(;;) {
					if (STATUS.compareAndSet(this, STATUS_READY, STATUS_REQUESTED) ||
							STATUS.compareAndSet(this, STATUS_PAUSED, STATUS_PAUSED_REQUESTED)){
						break;
					}
				}
			}
		}

		@Override
		boolean ready() {
			return rounds == 0L && (status == STATUS_REQUESTED || status == STATUS_READY);
		}

		@Override
		boolean isPaused() {
			return super.isPaused() || status == STATUS_PAUSED_REQUESTED;
		}
	}
}
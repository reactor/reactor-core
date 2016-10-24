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

package reactor.test.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.concurrent.QueueSupplier;

/**
 * A {@link TimedScheduler} that uses a virtual clock, allowing to manipulate time
 * (eg. in tests). Can replace the default reactor schedulers by using 
 * the {@link #enable(boolean)} method.
 *
 * @author Stephane Maldini
 */
public class VirtualTimeScheduler implements TimedScheduler {

	/**
	 * @return a new {@link VirtualTimeScheduler}
	 */
	public static VirtualTimeScheduler create() {
		return new VirtualTimeScheduler();
	}

	/**
	 * Assign a single newly created {@link VirtualTimeScheduler} to all or timed-only
	 * {@link Schedulers.Factory} factories. While the method is thread safe, its usually
	 * advised to execute such wide-impact BEFORE all tested code runs (setup etc).
	 * The created scheduler is returned.
	 *
	 * @param allSchedulers true if all {@link Schedulers.Factory} factories
	 * @return the VirtualTimeScheduler that was created and set through the factory
	 */
	public static VirtualTimeScheduler enable(boolean allSchedulers) {
		//TODO in 3.0.3 setting a factory will shutdown the previous Schedulers
		Schedulers.shutdownNow();
		VirtualTimeScheduler s = new VirtualTimeScheduler();
		if (!allSchedulers) {
			Schedulers.setFactory(new TimedOnlyFactory(s));
		}
		else {
			Schedulers.setFactory(new AllFactory(s));
		}
		return s;
	}

	/**
	 * The current {@link VirtualTimeScheduler} assigned in {@link Schedulers}
	 * @return current {@link VirtualTimeScheduler} assigned in {@link Schedulers}
	 * @throws IllegalStateException if no {@link VirtualTimeScheduler} has been found
	 */
	public static VirtualTimeScheduler get(){
		Scheduler s = Schedulers.newTimer("");
		if(s instanceof VirtualTimeScheduler){
			@SuppressWarnings("unchecked")
			VirtualTimeScheduler _s = (VirtualTimeScheduler)s;
			return _s;
		}
		throw new IllegalStateException("Check if VirtualTimeScheduler#enable has been invoked" +
				" first" +
				": "+s);
	}

	/**
	 * Re-assign the default Reactor Core {@link Schedulers} factories.
	 * While the method is thread safe, its usually advised to execute such wide-impact
	 * AFTER all tested code has been run (teardown etc).
	 */
	public static void reset() {
		Schedulers.shutdownNow();
		Schedulers.resetFactory();
	}

	final Queue<TimedRunnable> queue =
			new PriorityBlockingQueue<>(QueueSupplier.XS_BUFFER_SIZE);

	volatile long counter;
	volatile long nanoTime;

	protected VirtualTimeScheduler() {
	}

	/**
	 * Triggers any tasks that have not yet been executed and that are scheduled to be
	 * executed at or before this {@link VirtualTimeScheduler}'s present time.
	 */
	public void advanceTime() {
		advanceTime(nanoTime);
	}

	/**
	 * Moves the {@link VirtualTimeScheduler}'s clock forward by a specified amount of time.
	 *
	 * @param delayTime the amount of time to move the {@link VirtualTimeScheduler}'s clock forward
	 */
	public void advanceTimeBy(Duration delayTime) {
		advanceTime(nanoTime + delayTime.toNanos());
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
		advanceTime(targetTime);
	}

	@Override
	public TimedWorker createWorker() {
		return new TestWorker();
	}

	@Override
	public long now(TimeUnit unit) {
		return unit.convert(nanoTime, TimeUnit.NANOSECONDS);
	}

	@Override
	public Cancellation schedule(Runnable task) {
		return createWorker().schedule(task);
	}

	@Override
	public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
		return createWorker().schedule(task, delay, unit);
	}

	@Override
	public Cancellation schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		final TimedWorker w = createWorker();

		PeriodicDirectTask periodicTask = new PeriodicDirectTask(task, w);

		w.schedulePeriodically(periodicTask, initialDelay, period, unit);

		return periodicTask;
	}

	final void advanceTime(long targetTimeInNanoseconds) {
		while (!queue.isEmpty()) {
			TimedRunnable current = queue.peek();
			if (current.time > targetTimeInNanoseconds) {
				break;
			}
			// if scheduled time is 0 (immediate) use current virtual time
			nanoTime = current.time == 0 ? nanoTime : current.time;
			queue.remove();

			// Only execute if not unsubscribed
			if (!current.scheduler.shutdown) {
				current.run.run();
			}
		}
		nanoTime = targetTimeInNanoseconds;
	}

	static final class TimedRunnable implements Comparable<TimedRunnable> {

		final long       time;
		final Runnable   run;
		final TestWorker scheduler;
		final long       count; // for differentiating tasks at same time

		TimedRunnable(TestWorker scheduler, long time, Runnable run, long count) {
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

	static final class TimedOnlyFactory implements Schedulers.Factory {

		final VirtualTimeScheduler s;

		public TimedOnlyFactory(VirtualTimeScheduler s) {
			this.s = s;
		}

		@Override
		public TimedScheduler newTimer(ThreadFactory threadFactory) {
			return s;
		}
	}

	static final class AllFactory implements Schedulers.Factory {

		final VirtualTimeScheduler s;

		public AllFactory(VirtualTimeScheduler s) {
			this.s = s;
		}

		@Override
		public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
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

		@Override
		public TimedScheduler newTimer(ThreadFactory threadFactory) {
			return s;
		}
	}

	final class TestWorker implements TimedWorker {

		volatile boolean shutdown;

		@Override
		public long now(TimeUnit unit) {
			return VirtualTimeScheduler.this.now(unit);
		}

		@Override
		public Cancellation schedule(Runnable run) {
			if (shutdown) {
				return REJECTED;
			}
			final TimedRunnable timedTask = new TimedRunnable(this,
					0,
					run,
					COUNTER.getAndIncrement(VirtualTimeScheduler.this));
			queue.add(timedTask);
			return () -> queue.remove(timedTask);
		}

		@Override
		public Cancellation schedule(Runnable run, long delayTime, TimeUnit unit) {
			if (shutdown) {
				return REJECTED;
			}
			final TimedRunnable timedTask = new TimedRunnable(this,
					nanoTime + unit.toNanos(delayTime),
					run,
					COUNTER.getAndIncrement(VirtualTimeScheduler.this));
			queue.add(timedTask);

			return () -> queue.remove(timedTask);
		}

		@Override
		public Cancellation schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			final long periodInNanoseconds = unit.toNanos(period);
			final long firstNowNanoseconds = now(TimeUnit.NANOSECONDS);
			final long firstStartInNanoseconds = firstNowNanoseconds + unit.toNanos(initialDelay);

			PeriodicTask periodicTask = new PeriodicTask(firstStartInNanoseconds, task,
					firstNowNanoseconds,
					periodInNanoseconds);

			replace(periodicTask, schedule(periodicTask, initialDelay, unit));

			return periodicTask;
		}

		@Override
		public void shutdown() {
			shutdown = true;
		}

	}

	static final Cancellation CANCELLED = () -> {
	};
	static final Cancellation EMPTY = () -> {
	};

	final class PeriodicTask extends AtomicReference<Cancellation> implements Runnable,
	                                                                          Cancellation {

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

			if (get() != CANCELLED) {

				long nextTick;

				long nowNanoseconds = now(TimeUnit.NANOSECONDS);
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

	static boolean replace(AtomicReference<Cancellation> ref, Cancellation c) {
		for (; ; ) {
			Cancellation current = ref.get();
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

	static class PeriodicDirectTask implements Runnable, Cancellation {

		final Runnable run;

		final Scheduler.Worker worker;

		volatile boolean disposed;

		PeriodicDirectTask(Runnable run, Worker worker) {
			this.run = run;
			this.worker = worker;
		}

		@Override
		public void run() {
			if (!disposed) {
				try {
					run.run();
				}
				catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					worker.shutdown();
					throw Exceptions.propagate(ex);
				}
			}
		}

		@Override
		public void dispose() {
			disposed = true;
			worker.shutdown();
		}
	}

	static final AtomicLongFieldUpdater<VirtualTimeScheduler> COUNTER =
			AtomicLongFieldUpdater.newUpdater(VirtualTimeScheduler.class, "counter");
	static final long CLOCK_DRIFT_TOLERANCE_NANOSECONDS;

	static {
		CLOCK_DRIFT_TOLERANCE_NANOSECONDS = TimeUnit.MINUTES.toNanos(Long.getLong(
				"reactor.scheduler.drift-tolerance",
				15));
	}


}

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

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import reactor.core.Cancellation;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.concurrent.QueueSupplier;

/**
 *
 *
 */
public class TestScheduler implements TimedScheduler {

	/**
	 * @return a new {@link TestScheduler}
	 */
	public static TestScheduler create() {
		return new TestScheduler();
	}

	/**
	 * Assign a singleton {@link TestScheduler} to all or timed-only {@link
	 * Schedulers.Factory} factories. While the method is thread safe, its usually advised
	 * to execute such wide-impact BEFORE all tested code runs (setup etc).
	 *
	 * @param allSchedulers true if all {@link Schedulers.Factory} factories
	 */
	public static void enable(boolean allSchedulers) {
		TestScheduler s = new TestScheduler();
		if (allSchedulers) {
			Schedulers.setFactory(new TimedOnlyFactory(s));
		}
		else {
			Schedulers.setFactory(new AllFactory(s));
		}
	}

	/**
	 * The current {@link TestScheduler} assigned in {@link Schedulers}
	 * @return current {@link TestScheduler} assigned in {@link Schedulers}
	 * @throws IllegalStateException if no {@link TestScheduler} has been found
	 */
	public static TestScheduler get(){
		Scheduler s = Schedulers.newSingle("");
		if(s instanceof TestScheduler){
			@SuppressWarnings("unchecked")
			TestScheduler _s = (TestScheduler)s;
			return _s;
		}
		throw new IllegalStateException("A TestScheduler has noot been returned from " +
				"Schedulers#timer, check if TestScheduler#enable has been invoked first" +
				": "+s);
	}

	/**
	 * Re-assign the default Reactor Core {@link Schedulers} factories.
	 * While the method is thread safe, its usually advised to execute such wide-impact
	 * AFTER all tested code has been run (teardown etc).
	 */
	public static void reset() {
		Schedulers.resetFactory();
	}

	final Queue<TimedRunnable> queue =
			new PriorityBlockingQueue<>(QueueSupplier.XS_BUFFER_SIZE);

	volatile long counter;
	volatile long nanoTime;

	protected TestScheduler() {
	}

	/**
	 * Triggers any tasks that have not yet been executed and that are scheduled to be
	 * executed at or before this {@link TestScheduler}'s present time.
	 */
	public void advanceTime() {
		advanceTime(nanoTime);
	}

	/**
	 * Moves the {@link TestScheduler}'s clock forward by a specified amount of time.
	 *
	 * @param delayTime the amount of time to move the {@link TestScheduler}'s clock forward
	 * @param unit the units of time that {@code delayTime} is expressed in
	 */
	public void advanceTimeBy(long delayTime, TimeUnit unit) {
		advanceTimeTo(nanoTime + unit.toNanos(delayTime), TimeUnit.NANOSECONDS);
	}

	/**
	 * Moves the {@link TestScheduler}'s clock to a particular moment in time.
	 *
	 * @param delayTime the point in time to move the {@link TestScheduler}'s clock to
	 * @param unit the units of time that {@code delayTime} is expressed in
	 */
	public void advanceTimeTo(long delayTime, TimeUnit unit) {
		long targetTime = unit.toNanos(delayTime);
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
		return createWorker().schedulePeriodically(task, initialDelay, period, unit);
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

		final TestScheduler s;

		public TimedOnlyFactory(TestScheduler s) {
			this.s = s;
		}

		@Override
		public TimedScheduler newTimer(ThreadFactory threadFactory) {
			return s;
		}
	}

	static final class AllFactory implements Schedulers.Factory {

		final TestScheduler s;

		public AllFactory(TestScheduler s) {
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
			return TestScheduler.this.now(unit);
		}

		@Override
		public Cancellation schedule(Runnable run) {
			if (shutdown) {
				return REJECTED;
			}
			final TimedRunnable timedTask = new TimedRunnable(this,
					0,
					run,
					COUNTER.getAndIncrement(TestScheduler.this));
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
					COUNTER.getAndIncrement(TestScheduler.this));
			queue.add(timedTask);

			return () -> queue.remove(timedTask);
		}

		@Override
		public Cancellation schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return null;
		}

		@Override
		public void shutdown() {
			shutdown = true;
		}

	}

	static final AtomicLongFieldUpdater<TestScheduler> COUNTER =
			AtomicLongFieldUpdater.newUpdater(TestScheduler.class, "counter");
}

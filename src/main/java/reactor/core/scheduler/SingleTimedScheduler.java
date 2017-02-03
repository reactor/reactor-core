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

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.util.concurrent.OpenHashSet;

import static reactor.core.scheduler.ExecutorServiceScheduler.CANCELLED;
import static reactor.core.scheduler.ExecutorServiceScheduler.FINISHED;

/**
 * A TimedScheduler with an embedded, single-threaded ScheduledExecutorService,
 * shared among all workers.
 */
final class SingleTimedScheduler implements TimedScheduler {

	static final AtomicLong COUNTER = new AtomicLong();

	final ScheduledExecutorService executor;

	/**
	 * Constructs a new SingleTimedScheduler with the given thread factory.
	 *
	 * @param threadFactory the thread factory to use
	 */
	SingleTimedScheduler(ThreadFactory threadFactory) {
		ScheduledExecutorService e = Executors.newScheduledThreadPool(1, threadFactory);
		((ScheduledThreadPoolExecutor) e).setRemoveOnCancelPolicy(true);
		executor = Schedulers.decorateScheduledExecutorService(Schedulers.TIMER, () -> e);
	}

	@Override
	public Disposable schedule(Runnable task) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.submit(task),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.schedule(task, delay, unit),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.scheduleAtFixedRate(task, initialDelay, period, unit),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public void start() {
		throw new UnsupportedOperationException("Not supported, yet.");
	}

	@Override
	public boolean isDisposed() {
		return executor.isShutdown();
	}

	@Override
	public void shutdown() {
		dispose();
	}

	@Override
	public void dispose() {
		Schedulers.executorServiceShutdown(executor, Schedulers.TIMER);
	}

	@Override
	public TimedWorker createWorker() {
		return new SingleTimedSchedulerWorker(executor);
	}

	static final class SingleTimedSchedulerWorker implements TimedWorker {

		final ScheduledExecutorService executor;

		OpenHashSet<CancelFuture> tasks;

		volatile boolean terminated;

		SingleTimedSchedulerWorker(ScheduledExecutorService executor) {
			this.executor = executor;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (terminated) {
				return REJECTED;
			}

			TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);

			synchronized (this) {
				if (terminated) {
					return REJECTED;
				}

				tasks.add(sr);
			}

			try {
				Future<?> f = executor.submit(sr);
				sr.set(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				return REJECTED;
			}

			return sr;
		}

		void delete(CancelFuture r) {
			synchronized (this) {
				if (!terminated) {
					tasks.remove(r);
				}
			}
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			if (terminated) {
				return REJECTED;
			}

			TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);

			synchronized (this) {
				if (terminated) {
					return REJECTED;
				}

				tasks.add(sr);
			}

			try {
				Future<?> f = executor.schedule(sr, delay, unit);
				sr.set(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				return REJECTED;
			}

			return sr;
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			if (terminated) {
				return REJECTED;
			}

			TimedPeriodicScheduledRunnable sr =
					new TimedPeriodicScheduledRunnable(task, this);

			synchronized (this) {
				if (terminated) {
					return REJECTED;
				}

				tasks.add(sr);
			}

			try {
				Future<?> f =
						executor.scheduleAtFixedRate(sr, initialDelay, period, unit);
				sr.set(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				return REJECTED;
			}

			return sr;
		}

		@Override
		public void shutdown() {
			dispose();
		}

		@Override
		public void dispose() {
			if (terminated) {
				return;
			}
			terminated = true;

			OpenHashSet<CancelFuture> set;

			synchronized (this) {
				set = tasks;
				if (set == null) {
					return;
				}
				tasks = null;
			}

			if (!set.isEmpty()) {
				Object[] keys = set.keys();
				for (Object c : keys) {
					if (c != null) {
						((CancelFuture) c).cancelFuture();
					}
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}
	}

	interface CancelFuture {

		void cancelFuture();
	}

	static final class TimedScheduledRunnable extends AtomicReference<Future<?>>
			implements Runnable, Disposable, CancelFuture {

		/** */
		private static final long serialVersionUID = 2284024836904862408L;

		final Runnable task;

		final SingleTimedSchedulerWorker parent;

		volatile Thread current;
		static final AtomicReferenceFieldUpdater<TimedScheduledRunnable, Thread> CURRENT =
				AtomicReferenceFieldUpdater.newUpdater(TimedScheduledRunnable.class,
						Thread.class,
						"current");

		TimedScheduledRunnable(Runnable task, SingleTimedSchedulerWorker parent) {
			this.task = task;
			this.parent = parent;
		}

		@Override
		public void run() {
			CURRENT.lazySet(this, Thread.currentThread());
			try {
				try {
					task.run();
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
				}
			}
			finally {
				for (; ; ) {
					Future<?> a = get();
					if (a == CANCELLED) {
						break;
					}
					if (compareAndSet(a, FINISHED)) {
						if (a != null) {
							doCancel(a);
						}
						parent.delete(this);
						break;
					}
				}
				CURRENT.lazySet(this, null);
			}
		}

		void doCancel(Future<?> a) {
			a.cancel(Thread.currentThread() != current);
		}

		@Override
		public void cancelFuture() {
			for (; ; ) {
				Future<?> a = get();
				if (a == FINISHED) {
					return;
				}
				if (compareAndSet(a, CANCELLED)) {
					if (a != null) {
						doCancel(a);
					}
					return;
				}
			}
		}

		@Override
		public void dispose() {
			for (; ; ) {
				Future<?> a = get();
				if (a == FINISHED) {
					return;
				}
				if (compareAndSet(a, CANCELLED)) {
					if (a != null) {
						doCancel(a);
					}
					parent.delete(this);
					return;
				}
			}
		}

		@Override
		public boolean isDisposed() {
			Future<?> a = get();
			return FINISHED == a || CANCELLED == a;
		}
	}

	static final class TimedPeriodicScheduledRunnable extends AtomicReference<Future<?>>
			implements Runnable, Disposable, CancelFuture {

		/** */
		private static final long serialVersionUID = 2284024836904862408L;

		final Runnable task;

		final SingleTimedSchedulerWorker parent;

		@SuppressWarnings("unused")
		volatile Thread current;
		static final AtomicReferenceFieldUpdater<TimedPeriodicScheduledRunnable, Thread>
				CURRENT = AtomicReferenceFieldUpdater.newUpdater(
				TimedPeriodicScheduledRunnable.class,
				Thread.class,
				"current");

		TimedPeriodicScheduledRunnable(Runnable task,
				SingleTimedSchedulerWorker parent) {
			this.task = task;
			this.parent = parent;
		}

		@Override
		public void run() {
			CURRENT.lazySet(this, Thread.currentThread());
			try {
				try {
					task.run();
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
					for (; ; ) {
						Future<?> a = get();
						if (a == CANCELLED) {
							break;
						}
						if (compareAndSet(a, FINISHED)) {
							parent.delete(this);
							break;
						}
					}
				}
			}
			finally {
				CURRENT.lazySet(this, null);
			}
		}

		void doCancel(Future<?> a) {
			a.cancel(false);
		}

		@Override
		public void cancelFuture() {
			for (; ; ) {
				Future<?> a = get();
				if (a == FINISHED) {
					return;
				}
				if (compareAndSet(a, CANCELLED)) {
					if (a != null) {
						doCancel(a);
					}
					return;
				}
			}
		}

		@Override
		public boolean isDisposed() {
			Future<?> a = get();
			return FINISHED == a || CANCELLED == a;
		}

		@Override
		public void dispose() {
			for (; ; ) {
				Future<?> a = get();
				if (a == FINISHED) {
					return;
				}
				if (compareAndSet(a, CANCELLED)) {
					if (a != null) {
						doCancel(a);
					}
					parent.delete(this);
					return;
				}
			}
		}
	}

}

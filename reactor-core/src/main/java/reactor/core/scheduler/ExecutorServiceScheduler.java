/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.concurrent.OpenHashSet;

/**
 * A simple {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
 * Runnables for async operators. This scheduler is time-capable (can schedule with a
 * delay and/or periodically) if the backing executor is a {@link ScheduledExecutorService}.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ExecutorServiceScheduler implements Scheduler {

	static final Runnable  EMPTY     = () -> {
	};
	static final Future<?> CANCELLED = new FutureTask<>(EMPTY, null);
	static final Future<?> FINISHED  = new FutureTask<>(EMPTY, null);

	final ExecutorService executor;
	final boolean         interruptOnCancel;

	ExecutorServiceScheduler(ExecutorService executorService, boolean interruptOnCancel) {
		if (executorService instanceof ScheduledExecutorService) {
			this.executor = Schedulers.decorateScheduledExecutorService("ExecutorService",
					() -> (ScheduledExecutorService) executorService);
		}
		else {
			this.executor = Schedulers.decorateExecutorService("ExecutorService",
					() -> executorService);
		}
		this.interruptOnCancel = interruptOnCancel;
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(executor, interruptOnCancel);
	}

	boolean isTimeCapable() {
		return executor instanceof ScheduledExecutorService;
	}

	@Override
	public Disposable schedule(Runnable task) {
		//RejectedExecutionException are propagated up
		return new DisposableFuture(executor.submit(task), interruptOnCancel);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		if (!isTimeCapable()) {
			throw Exceptions.failWithRejected();
		}

		ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

		//RejectedExecutionException are propagated up
		return new DisposableFuture(scheduledExecutor.schedule(task, delay, unit), interruptOnCancel);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		if (!isTimeCapable()) {
			throw Exceptions.failWithRejected();
		}

		ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

		//RejectedExecutionException are propagated up
		return new DisposableFuture(scheduledExecutor.scheduleAtFixedRate(task, initialDelay, period, unit), interruptOnCancel);
	}

	@Override
	public boolean isDisposed() {
		return executor.isShutdown();
	}

	@Override
	public void dispose() {
		Schedulers.executorServiceShutdown(executor, "ExecutorService");
	}

	static final class DisposableFuture implements Disposable {

		final Future<?> f;
		final boolean   interruptOnCancel;

		DisposableFuture(Future<?> f, boolean interruptOnCancel) {
			this.f = f;
			this.interruptOnCancel = interruptOnCancel;
		}

		@Override
		public void dispose() {
			f.cancel(interruptOnCancel);
		}

		@Override
		public boolean isDisposed() {
			return f.isCancelled() || f.isDone();
		}
	}

	static final class ExecutorServiceWorker implements Worker,
	                                                    Composite<ExecutorServiceSchedulerRunnable> {

		final ExecutorService executor;
		final boolean         interruptOnCancel;

		volatile boolean terminated;

		OpenHashSet<ExecutorServiceSchedulerRunnable> tasks;

		ExecutorServiceWorker(ExecutorService executor, boolean interruptOnCancel) {
			this.executor = executor;
			this.interruptOnCancel = interruptOnCancel;
			this.tasks = new OpenHashSet<>();
		}

		boolean isTimeCapable() {
			return executor instanceof ScheduledExecutorService;
		}

		@Override
		public Disposable schedule(Runnable t) {
			ExecutorServiceSchedulerRunnable sr = new ExecutorServiceSchedulerRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = executor.submit(sr);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				dispose(sr);
				//RejectedExecutionException are propagated up
				throw ree;
			}
			throw Exceptions.failWithRejected();
		}

		@Override
		public Disposable schedule(Runnable t, long delay, TimeUnit unit) {
			if (!isTimeCapable()) {
				throw Exceptions.failWithRejected();
			}

			ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

			ExecutorServiceSchedulerRunnable sr = new ExecutorServiceSchedulerRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = scheduledExecutor.schedule(sr, delay, unit);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				dispose(sr);
				//RejectedExecutionException are propagated up
				throw ree;
			}
			throw Exceptions.failWithRejected();
		}

		@Override
		public Disposable schedulePeriodically(Runnable t, long initialDelay, long period, TimeUnit unit) {
			if (!isTimeCapable()) {
				throw Exceptions.failWithRejected();
			}

			ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

			ExecutorServiceSchedulerRunnable sr = new ExecutorServiceSchedulerRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = scheduledExecutor.scheduleAtFixedRate(sr, initialDelay, period, unit);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				dispose(sr);
				//RejectedExecutionException are propagated up
				throw ree;
			}
			//TODO for case where not added but not rejected, simplify these patterns?
			throw Exceptions.failWithRejected();
		}



		@Override
		public boolean add(ExecutorServiceSchedulerRunnable sr) {
			if (!terminated) {
				synchronized (this) {
					if (!terminated) {
						tasks.add(sr);
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public boolean remove(ExecutorServiceSchedulerRunnable sr) {
			if (!terminated) {
				synchronized (this) {
					if (!terminated) {
						tasks.remove(sr);
						return true;
					}
				}
			}
			return false;
		}

		/**
		 * Remove the {@link Disposable} from this container and dispose it via
		 * {@link Disposable#dispose() dispose()} once deleted.
		 *
		 * @param sr the {@link Disposable} to remove and dispose.
		 * @return true if the disposable was successfully removed and disposed, false otherwise.
		 */
		private boolean dispose(ExecutorServiceSchedulerRunnable sr) {
			if (remove(sr)) {
				sr.dispose();
				return true;
			}
			return false;
		}

		@Override
		public int size() {
			return tasks.size();
		}

		@Override
		public void dispose() {
			if (!terminated) {
				OpenHashSet<ExecutorServiceSchedulerRunnable> coll;
				synchronized (this) {
					if (terminated) {
						return;
					}
					coll = tasks;
					tasks = null;
					terminated = true;
				}

				if (!coll.isEmpty()) {
					Object[] a = coll.keys();
					for (Object o : a) {
						if (o != null) {
							((ExecutorServiceSchedulerRunnable) o).dispose();
						}
					}
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}
	}

	/**
	 * A runnable task for {@link ExecutorServiceScheduler} Workers that exposes the
	 * ability to cancel inner task when interrupted.
	 *
	 * @author Simon Baslé
	 */
	static final class ExecutorServiceSchedulerRunnable implements Runnable, Disposable {

		@SuppressWarnings("ConstantConditions")
		static final ExecutorServiceWorker DISPOSED_PARENT = new ExecutorServiceWorker(null, false);
		@SuppressWarnings("ConstantConditions")
		static final ExecutorServiceWorker DONE_PARENT     = new ExecutorServiceWorker(null, false);

		final Runnable task;

		volatile Future<?> future;
		static final AtomicReferenceFieldUpdater<ExecutorServiceSchedulerRunnable, Future>
				FUTURE = AtomicReferenceFieldUpdater.newUpdater(
				ExecutorServiceSchedulerRunnable.class,
				Future.class,
				"future");

		volatile ExecutorServiceWorker parent;
		static final AtomicReferenceFieldUpdater<ExecutorServiceSchedulerRunnable, ExecutorServiceWorker>
				PARENT = AtomicReferenceFieldUpdater.newUpdater(
				ExecutorServiceSchedulerRunnable.class,
				ExecutorServiceWorker.class,
				"parent");

		ExecutorServiceSchedulerRunnable(Runnable task, ExecutorServiceWorker parent) {
			this.task = task;
			PARENT.lazySet(this, parent);
		}

		@Override
		public void run() {
			try {
				try {
					task.run();
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
				}
			}
			finally {
				ExecutorServiceWorker o = parent;
				if (o != DISPOSED_PARENT && o != null && PARENT.compareAndSet(this, o, DONE_PARENT)) {
					o.remove(this);
				}

				Future f;
				for (; ; ) {
					f = future;
					if (f == CANCELLED || FUTURE.compareAndSet(this, f, FINISHED)) {
						break;
					}
				}
			}
		}

		void setFuture(Future<?> f) {
			for (; ; ) {
				Future o = future;
				if (o == FINISHED) {
					return;
				}
				if (o == CANCELLED) {
					f.cancel(parent.interruptOnCancel);
					return;
				}
				if (FUTURE.compareAndSet(this, o, f)) {
					return;
				}
			}
		}

		@Override
		public boolean isDisposed() {
			Future<?> a = future;
			return FINISHED == a || CANCELLED == a;
		}

		@Override
		public void dispose() {
			for (; ; ) {
				Future f = future;
				if (f == FINISHED || f == CANCELLED) {
					break;
				}
				if (FUTURE.compareAndSet(this, f, CANCELLED)) {
					if (f != null) {
						f.cancel(parent.interruptOnCancel);
					}
					break;
				}
			}

			for (; ; ) {
				ExecutorServiceWorker o = parent;
				if (o == DONE_PARENT || o == DISPOSED_PARENT || o == null) {
					return;
				}
				if (PARENT.compareAndSet(this, o, DISPOSED_PARENT)) {
					o.remove(this);
					return;
				}
			}
		}
	}
}

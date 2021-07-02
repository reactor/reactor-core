/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Wraps a java.util.concurrent.Executor and provides the Scheduler API over it.
 * <p>
 * It supports both non-trampolined worker (for cases where the trampolining happens
 * externally) and trampolined worker. This scheduler is NOT time-capable (can't schedule
 * with delay / periodically).
 *
 * @author Stephane Maldini
 */
final class ExecutorScheduler implements Scheduler, Scannable {

	final Executor executor;
	final boolean  trampoline;

	volatile boolean terminated;

	ExecutorScheduler(Executor executor, boolean trampoline) {
		this.executor = executor;
		this.trampoline = trampoline;
	}

	@Override
	public Disposable schedule(Runnable task) {
		if(terminated){
			throw Exceptions.failWithRejected();
		}
		Objects.requireNonNull(task, "task");
		ExecutorPlainRunnable r = new ExecutorPlainRunnable(task);
		//RejectedExecutionException are propagated up, but since Executor doesn't from
		//failing tasks we'll also wrap the execute call in a try catch:
		try {
			executor.execute(r);
		}
		catch (Throwable ex) {
			if (executor instanceof ExecutorService && ((ExecutorService) executor).isShutdown()) {
				terminated = true;
			}
			Schedulers.handleError(ex);
			throw Exceptions.failWithRejected(ex);
		}
		return r;
	}

	@Override
	public void dispose() {
		terminated = true;
	}

	@Override
	public boolean isDisposed() {
		return terminated;
	}

	@Override
	public Worker createWorker() {
		return trampoline ? new ExecutorSchedulerTrampolineWorker(executor) :
				new ExecutorSchedulerWorker(executor);
	}

	@Override
	public String toString() {
		StringBuilder ts = new StringBuilder(Schedulers.FROM_EXECUTOR)
				.append('(').append(executor);
		if (trampoline) ts.append(",trampolining");
		ts.append(')');

		return ts.toString();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.NAME) return toString();

		return null;
	}

	/**
	 * A non-tracked runnable that wraps a task and offers cancel support in the form
	 * of not executing the task.
	 * <p>Since Executor doesn't have cancellation support of its own, the
	 * ExecutorRunnable will stay in the Executor's queue and be always executed.
	 */
	static final class ExecutorPlainRunnable extends AtomicBoolean
			implements Runnable, Disposable {

		/** */
		private static final long serialVersionUID = 5116223460201378097L;

		final Runnable task;

		ExecutorPlainRunnable(Runnable task) {
			this.task = task;
		}

		@Override
		public void run() {
				if (!get()) {
					try {
						task.run();
					}
					catch (Throwable ex) {
						Schedulers.handleError(ex);
					}
					finally {
						lazySet(true);
					}
				}
		}

		@Override
		public boolean isDisposed() {
			return get();
		}

		@Override
		public void dispose() {
			set(true);
		}
	}

	/**
	 * Common interface between the tracking workers to signal the need for removal.
	 */
	interface WorkerDelete {

		void delete(ExecutorTrackedRunnable r);
	}

	/**
	 * A Runnable that wraps a task and has reference back to its parent worker to
	 * remove itself once completed or cancelled
	 */
	static final class ExecutorTrackedRunnable extends AtomicBoolean
			implements Runnable, Disposable {

		/** */
		private static final long serialVersionUID = 3503344795919906192L;

		final Runnable     task;
		final WorkerDelete parent;

		final boolean callRemoveOnFinish;

		ExecutorTrackedRunnable(Runnable task,
				WorkerDelete parent,
				boolean callRemoveOnFinish) {
			this.task = task;
			this.parent = parent;
			this.callRemoveOnFinish = callRemoveOnFinish;
		}

		@Override
		public void run() {
				if (!get()) {
					try {
						task.run();
					}
					catch (Throwable ex) {
						Schedulers.handleError(ex);
					}
					finally {
						if (callRemoveOnFinish) {
							dispose();
						}
						else {
							lazySet(true);
						}
					}
				}
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				parent.delete(this);
			}
		}

		@Override
		public boolean isDisposed() {
			return get();
		}
	}

	/**
	 * A non-trampolining worker that tracks tasks.
	 */
	static final class ExecutorSchedulerWorker implements Scheduler.Worker, WorkerDelete, Scannable {

		final Executor executor;

		final Disposable.Composite tasks;

		ExecutorSchedulerWorker(Executor executor) {
			this.executor = executor;
			this.tasks = Disposables.composite();
		}

		@Override
		public Disposable schedule(Runnable task) {
			Objects.requireNonNull(task, "task");

			ExecutorTrackedRunnable r = new ExecutorTrackedRunnable(task, this, true);
			if (!tasks.add(r)) {
				throw Exceptions.failWithRejected();
			}

			try {
				executor.execute(r);
			}
			catch (Throwable ex) {
				tasks.remove(r);
				Schedulers.handleError(ex);
				throw Exceptions.failWithRejected(ex);
			}

			return r;
		}

		@Override
		public void dispose() {
			tasks.dispose();
		}

		@Override
		public boolean isDisposed() {
			return tasks.isDisposed();
		}

		@Override
		public void delete(ExecutorTrackedRunnable r) {
			tasks.remove(r);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.BUFFERED) return tasks.size();
			if (key == Attr.PARENT) return (executor instanceof Scannable) ? executor : null;
			if (key == Attr.NAME) {
				//hack to recognize the SingleWorker
				if (executor instanceof SingleWorkerScheduler) return executor + ".worker";
				return Schedulers.FROM_EXECUTOR + "("  + executor + ").worker";
			}

			return Schedulers.scanExecutor(executor, key);
		}
	}

	/**
	 * A trampolining worker that tracks tasks.
	 */
	static final class ExecutorSchedulerTrampolineWorker
			implements Scheduler.Worker, WorkerDelete, Runnable, Scannable {

		final Executor executor;

		final Queue<ExecutorTrackedRunnable> queue;

		volatile boolean terminated;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<ExecutorSchedulerTrampolineWorker> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ExecutorSchedulerTrampolineWorker.class,
						"wip");

		ExecutorSchedulerTrampolineWorker(Executor executor) {
			this.executor = executor;
			this.queue = new ConcurrentLinkedQueue<>();
		}

		@Override
		public Disposable schedule(Runnable task) {
			Objects.requireNonNull(task, "task");
			if (terminated) {
				throw Exceptions.failWithRejected();
			}

			ExecutorTrackedRunnable r = new ExecutorTrackedRunnable(task, this, false);
			synchronized (this) {
				if (terminated) {
					throw Exceptions.failWithRejected();
				}
				queue.offer(r);
			}

			if (WIP.getAndIncrement(this) == 0) {
				try {
					executor.execute(this);
				}
				catch (Throwable ex) {
					r.dispose();
					Schedulers.handleError(ex);
					throw Exceptions.failWithRejected(ex);
				}
			}

			return r;
		}

		@Override
		public void dispose() {
			if (terminated) {
				return;
			}
			terminated = true;
			final Queue<ExecutorTrackedRunnable> q = queue;

			ExecutorTrackedRunnable r;

			while ((r = q.poll()) != null && !q.isEmpty()) {
				r.dispose();
			}
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}

		@Override
		public void delete(ExecutorTrackedRunnable r) {
			synchronized (this) {
				if (!terminated) {
					queue.remove(r);
				}
			}
		}

		@Override
		public void run() {
			final Queue<ExecutorTrackedRunnable> q = queue;

			for (; ; ) {

				int e = 0;
				int r = wip;

				while (e != r) {
					if (terminated) {
						return;
					}
					ExecutorTrackedRunnable task = q.poll();

					if (task == null) {
						break;
					}

					task.run();

					e++;
				}

				if (e == r && terminated) {
					return;
				}

				if (WIP.addAndGet(this, -e) == 0) {
					break;
				}
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.PARENT) return (executor instanceof Scannable) ? executor : null;
			if (key == Attr.NAME) return Schedulers.FROM_EXECUTOR + "("  + executor + ",trampolining).worker";
			if (key == Attr.BUFFERED || key == Attr.LARGE_BUFFERED) return queue.size();

			return Schedulers.scanExecutor(executor, key);
		}
	}

}

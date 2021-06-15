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

package reactor.core.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 */
final class ExecutorServiceWorker implements Scheduler.Worker, Disposable, Scannable {

	/**
	 * The {@link ScheduledExecutorService} that backs this worker (can be shared)
	 */
	final ScheduledExecutorService exec;

	/**
	 * Cleanup tasks to be performed when this worker is {@link Disposable#dispose() disposed},
	 * including but not limited to tasks that have been scheduled on the worker.
	 */
	final Composite disposables;
	
	CompletableFuture<Void> lastFuture;

	ExecutorServiceWorker(ScheduledExecutorService exec) {
		this.exec = exec;
		this.disposables = Disposables.composite();
	}

	@Override
	public Disposable schedule(Runnable task) {
		return workerSchedule(exec, disposables, task, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return workerSchedule(exec, disposables, task, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.workerSchedulePeriodically(exec, disposables,
				task,
				initialDelay,
				period,
				unit);
	}
	
	private Disposable workerSchedule(ScheduledExecutorService exec,
			Disposable.Composite tasks,
			Runnable task,
			long delay,
			TimeUnit unit) {
		
		task = Schedulers.onSchedule(task);

		WorkerTask sr = new WorkerTask(task, tasks);
		if (!tasks.add(sr)) {
			throw Exceptions.failWithRejected();
		}

		try {
			Future<?> f;
			if (delay <= 0L) {
				synchronized (this) {
					if (lastFuture != null) {
						lastFuture = lastFuture
								.whenComplete((res, ex) -> {})
								.thenRunAsync(sr, exec);
					} else {
						lastFuture = CompletableFuture.runAsync(sr, exec);
					}
				}
				f = lastFuture;
			}
			else {
				f = exec.schedule((Callable<?>) sr, delay, unit);
			}
			sr.setFuture(f);
		}
		catch (RejectedExecutionException ex) {
			sr.dispose();
			//RejectedExecutionException are propagated up
			throw ex;
		}

		return sr;
	}

	@Override
	public void dispose() {
		disposables.dispose();
	}

	@Override
	public boolean isDisposed() {
		return disposables.isDisposed();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) return disposables.size();
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.NAME) return "ExecutorServiceWorker"; //could be parallel, single or fromExecutorService

		return Schedulers.scanExecutor(exec, key);
	}
}

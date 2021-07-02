/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import reactor.core.Disposable;
import reactor.core.Disposables;
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


	ExecutorServiceWorker(ScheduledExecutorService exec) {
		this.exec = exec;
		this.disposables = Disposables.composite();
	}

	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.workerSchedule(exec, disposables, task, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.workerSchedule(exec, disposables, task, delay, unit);
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

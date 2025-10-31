/*
 * Copyright (c) 2017-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

final class PeriodicSchedulerTask implements Runnable, Disposable, Callable<Void> {

	final Runnable task;

	@SuppressWarnings("DataFlowIssue") // only a dummy object
	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	volatile @Nullable Future<?> future;

	// https://github.com/uber/NullAway/issues/1157
	@SuppressWarnings({"rawtypes", "DataFlowIssue"})
	static final AtomicReferenceFieldUpdater<PeriodicSchedulerTask, @Nullable Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(PeriodicSchedulerTask.class, Future.class, "future");

	@Nullable Thread thread;

	PeriodicSchedulerTask(Runnable task) {
		this.task = task;
	}

	@Override
	public @Nullable Void call() {
		thread = Thread.currentThread();
		try {
			try {
				task.run();
			}
			catch (Throwable ex) {
				Schedulers.handleError(ex);
			}
		}
		finally {
			thread = null;
		}
		return null;
	}

	@Override
	public void run() {
		call();
	}

	void setFuture(Future<?> f) {
		for (;;) {
			Future<?> o = future;
			if (o == CANCELLED) {
				f.cancel(thread != Thread.currentThread());
				return;
			}
			if (FUTURE.compareAndSet(this, o, f)) {
				return;
			}
		}
	}

	@Override
	public boolean isDisposed() {
		return future == CANCELLED;
	}

	@Override
	public void dispose() {
		for (;;) {
			Future<?> f = future;
			if (f == CANCELLED) {
				break;
			}
			if (FUTURE.compareAndSet(this, f, CANCELLED)) {
				if (f != null) {
					f.cancel(thread != Thread.currentThread());
				}
				break;
			}
		}
	}
}

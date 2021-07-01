/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A runnable task for {@link Scheduler} Workers that can run periodically
 **/
final class PeriodicWorkerTask implements Runnable, Disposable, Callable<Void> {

	final Runnable task;

	static final Composite DISPOSED = new EmptyCompositeDisposable();

	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<PeriodicWorkerTask, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(PeriodicWorkerTask.class, Future.class, "future");

	volatile Composite parent;
	static final AtomicReferenceFieldUpdater<PeriodicWorkerTask, Composite> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(PeriodicWorkerTask.class, Composite.class, "parent");

	Thread thread;

	PeriodicWorkerTask(Runnable task, Composite parent) {
		this.task = task;
		PARENT.lazySet(this, parent);
	}

	@Override
	@Nullable
	public Void call() {
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
			Future o = future;
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
			Future f = future;
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

		for (;;) {
			Composite o = parent;
			if (o == DISPOSED || o == null) {
				return;
			}
			if (PARENT.compareAndSet(this, o, DISPOSED)) {
				o.remove(this);
				return;
			}
		}
	}

}

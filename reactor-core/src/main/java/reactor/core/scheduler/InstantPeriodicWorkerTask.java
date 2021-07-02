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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

/**
 * A runnable task for {@link Scheduler} Workers that can run periodically
 **/
final class InstantPeriodicWorkerTask implements Disposable, Callable<Void> {

	final Runnable task;

	final ExecutorService executor;

	static final Composite DISPOSED = new EmptyCompositeDisposable();

	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	volatile Future<?>                                                          rest;
	static final AtomicReferenceFieldUpdater<InstantPeriodicWorkerTask, Future> REST =
			AtomicReferenceFieldUpdater.newUpdater(InstantPeriodicWorkerTask.class, Future.class, "rest");

	volatile Future<?>                                                          first;
	static final AtomicReferenceFieldUpdater<InstantPeriodicWorkerTask, Future> FIRST =
			AtomicReferenceFieldUpdater.newUpdater(InstantPeriodicWorkerTask.class, Future.class, "first");

	volatile Composite                                                             parent;
	static final AtomicReferenceFieldUpdater<InstantPeriodicWorkerTask, Composite> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(InstantPeriodicWorkerTask.class, Composite.class, "parent");

	Thread thread;

	InstantPeriodicWorkerTask(Runnable task, ExecutorService executor) {
		this.task = task;
		this.executor = executor;
	}

	InstantPeriodicWorkerTask(Runnable task, ExecutorService executor, Composite parent) {
		this.task = task;
		this.executor = executor;
		PARENT.lazySet(this, parent);
	}

	@Override
	@Nullable
	public Void call() {
		thread = Thread.currentThread();
		try {
			try {
				task.run();
				setRest(executor.submit(this));
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

	void setRest(Future<?> f) {
		for (;;) {
			Future o = rest;
			if (o == CANCELLED) {
				f.cancel(thread != Thread.currentThread());
				return;
			}
			if (REST.compareAndSet(this, o, f)) {
				return;
			}
		}
	}

	void setFirst(Future<?> f) {
		for (;;) {
			Future o = first;
			if (o == CANCELLED) {
				f.cancel(thread != Thread.currentThread());
				return;
			}
			if (FIRST.compareAndSet(this, o, f)) {
				return;
			}
		}
	}

	@Override
	public boolean isDisposed() {
		return rest == CANCELLED;
	}

	@Override
	public void dispose() {
		for (;;) {
			Future f = first;
			if (f == CANCELLED) {
				break;
			}
			if (FIRST.compareAndSet(this, f, CANCELLED)) {
				if (f != null) {
					f.cancel(thread != Thread.currentThread());
				}
				break;
			}
		}

		for (;;) {
			Future f = rest;
			if (f == CANCELLED) {
				break;
			}
			if (REST.compareAndSet(this, f, CANCELLED)) {
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

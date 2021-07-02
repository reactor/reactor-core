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
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.Nullable;

final class SchedulerTask implements Runnable, Disposable, Callable<Void> {

	final Runnable task;

	static final Future<Void> FINISHED = new FutureTask<>(() -> null);
	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	static final Disposable TAKEN = Disposables.disposed();

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<SchedulerTask, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(SchedulerTask.class, Future.class, "future");

	volatile Disposable parent;
	static final AtomicReferenceFieldUpdater<SchedulerTask, Disposable> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(SchedulerTask.class, Disposable.class, "parent");

	Thread thread;

	SchedulerTask(Runnable task, @Nullable Disposable parent) {
		this.task = task;
		PARENT.lazySet(this, parent);
	}

	@Override
	@Nullable
	public Void call() {
		thread = Thread.currentThread();
		Disposable d = null;
		try {
			for (;;) {
				d = parent;
				if (d == TAKEN || d == null) {
					break;
				}
				if (PARENT.compareAndSet(this, d, TAKEN)) {
					break;
				}
			}
			try {
				task.run();
			}
			catch (Throwable ex) {
				Schedulers.handleError(ex);
			}
		}
		finally {
			thread = null;
			Future f;
			for (;;) {
				f = future;
				if (f == CANCELLED || FUTURE.compareAndSet(this, f, FINISHED)) {
					break;
				}
			}
			if (d != null) {
				d.dispose();
			}
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
			if (o == FINISHED) {
				return;
			}
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
		Future<?> a = future;
		return FINISHED == a || CANCELLED == a;
	}

	@Override
	public void dispose() {
		for (;;) {
			Future f = future;
			if (f == FINISHED || f == CANCELLED) {
				break;
			}
			if (FUTURE.compareAndSet(this, f, CANCELLED)) {
				if (f != null) {
					f.cancel(thread != Thread.currentThread());
				}
				break;
			}
		}

		Disposable d;
		for (;;) {
			d = parent;
			if (d == TAKEN || d == null) {
				break;
			}
			if (PARENT.compareAndSet(this, d, TAKEN)) {
				d.dispose();
				break;
			}
		}
	}
}

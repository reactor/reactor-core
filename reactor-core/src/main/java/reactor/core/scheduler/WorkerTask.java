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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.Nullable;

import reactor.core.Disposable;

/**
 * A runnable task for {@link Scheduler} Workers that are time-capable (implementing a
 * relevant schedule(delay) and schedulePeriodically(period) methods).
 *
 * Unlike the one in {@link DelegateServiceScheduler}, this runnable doesn't expose the
 * ability to cancel inner task when interrupted.
 *
 * @author Simon Baslé
 * @author David Karnok
 */
final class WorkerTask implements Runnable, Disposable, Callable<Void> {

	final Runnable task;

	static final Composite DISPOSED = new EmptyCompositeDisposable();
	static final Composite DONE     = new EmptyCompositeDisposable();


	static final Future<Void> FINISHED = new FutureTask<>(() -> null);
	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<WorkerTask, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(WorkerTask.class, Future.class, "future");

	volatile Composite parent;
	static final AtomicReferenceFieldUpdater<WorkerTask, Composite> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(WorkerTask.class, Composite.class, "parent");

	Thread thread;

	WorkerTask(Runnable task, Composite parent) {
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
			Composite o = parent;
			if (o != DISPOSED && o != null && PARENT.compareAndSet(this, o, DONE)) {
				o.remove(this);
			}

			Future f;
			for (;;) {
				f = future;
				if (f == CANCELLED || FUTURE.compareAndSet(this, f, FINISHED)) {
					break;
				}
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

		for (;;) {
			Composite o = parent;
			if (o == DONE || o == DISPOSED || o == null) {
				return;
			}
			if (PARENT.compareAndSet(this, o, DISPOSED)) {
				o.remove(this);
				return;
			}
		}
	}

	static final class EmptyCompositeDisposable implements Composite {

		@Override
		public boolean add(Disposable d) {
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends Disposable> ds) {
			return false;
		}

		@Override
		public boolean remove(Disposable d) {
			return false;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public void dispose() {	}

		@Override
		public boolean isDisposed() {
			return false;
		}

	}
}

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

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;

import static reactor.core.scheduler.ExecutorServiceScheduler.CANCELLED;
import static reactor.core.scheduler.ExecutorServiceScheduler.FINISHED;

/**
 * A runnable task for {@link Scheduler} Workers that are time-capable (implementing a
 * relevant schedule(delay) and schedulePeriodically(period) methods).
 *
 * Unlike the one in {@link ExecutorServiceScheduler}, this runnable doesn't expose the
 * ability to cancel inner task when interrupted.
 *
 * @author Simon Baslé
 */
final class ScheduledRunnable implements Runnable, Disposable {

	final Runnable task;

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<ScheduledRunnable, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(ScheduledRunnable.class, Future.class, "future");

	volatile Composite parent;
	static final AtomicReferenceFieldUpdater<ScheduledRunnable, Composite> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(ScheduledRunnable.class, Composite.class, "parent");

	ScheduledRunnable(Runnable task, Composite parent) {
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
			Composite o = parent;
			if (o != EmptyCompositeDisposable.DISPOSED_PARENT && o != null && PARENT.compareAndSet(this, o, EmptyCompositeDisposable.DONE_PARENT)) {
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
	}

	void setFuture(Future<?> f) {
		for (;;) {
			Future o = future;
			if (o == FINISHED) {
				return;
			}
			if (o == CANCELLED) {
				f.cancel(true);
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
					f.cancel(true);
				}
				break;
			}
		}

		for (;;) {
			Composite o = parent;
			if (o == EmptyCompositeDisposable.DONE_PARENT || o == EmptyCompositeDisposable.DISPOSED_PARENT || o == null) {
				return;
			}
			if (PARENT.compareAndSet(this, o, EmptyCompositeDisposable.DISPOSED_PARENT)) {
				o.remove(this);
				return;
			}
		}
	}

}

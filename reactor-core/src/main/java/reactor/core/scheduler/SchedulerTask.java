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
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.ContextAware;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class SchedulerTask implements Scheduler.ContextRunnable, Disposable, Callable<Void> {

	final Runnable task;

	static final Future<Void> FINISHED = new FutureTask<>(() -> null);
	static final Future<Void> CANCELLED = new FutureTask<>(() -> null);

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<SchedulerTask, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(SchedulerTask.class, Future.class, "future");

	Thread thread;

	SchedulerTask(Runnable task) {
		this.task = task;
	}

	@Override
	public Context currentContext() {
		return task instanceof ContextAware ? ((ContextAware) task).currentContext() : Context.empty();
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
	}
}

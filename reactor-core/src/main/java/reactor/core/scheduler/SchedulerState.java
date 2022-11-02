/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import static reactor.core.scheduler.SchedulerState.DisposeAwaiterRunnable.awaitInPool;

final class SchedulerState<T> {

	@Nullable
	final T initialResource;
	final T currentResource;
	final Mono<Void> onDispose;

	private SchedulerState(@Nullable T initialResource, T currentResource, Mono<Void> onDispose) {
		this.initialResource = initialResource;
		this.currentResource = currentResource;
		this.onDispose = onDispose;
	}

	static <T> SchedulerState<T> init(final T resource) {
		return new SchedulerState<>(resource, resource, Mono.empty());
	}

	static <T> SchedulerState<T> transition(@Nullable T initial, T next, DisposeAwaiter<T> awaiter) {
		return new SchedulerState<T>(
			initial,
			next,
			initial == null ? Mono.empty() :
				Flux.<Void>create(sink -> awaitInPool(awaiter, initial, sink, 1000, 100))
					.replay()
					.refCount()
					.next());
	}

	interface DisposeAwaiter<T> {

		boolean await(T resource, long timeout, TimeUnit timeUnit) throws InterruptedException;

		boolean tryAwait(T resource);
	}

	static class DisposeAwaiterRunnable<T> implements Runnable {

		static final ScheduledExecutorService TRANSITION_AWAIT_POOL;

		static {
			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
			executor.setKeepAliveTime(10, TimeUnit.SECONDS);
			executor.allowCoreThreadTimeOut(true);
			executor.setMaximumPoolSize(Schedulers.DEFAULT_POOL_SIZE);
			TRANSITION_AWAIT_POOL = executor;
		}

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DisposeAwaiterRunnable> STATE =
			AtomicIntegerFieldUpdater.newUpdater(DisposeAwaiterRunnable.class, "state");

		private final DisposeAwaiter<T> awaiter;
		private final T                 initial;
		private final int               initialAwaitMs;
		private final int               retryDelay;
		private final FluxSink<Void>    sink;

		volatile int state;

		static <R> void awaitInPool(DisposeAwaiter<R> awaiter, R initial, FluxSink<Void> sink, int initialAwaitMs, int retryDelayMs) {
			DisposeAwaiterRunnable<R> poller = new DisposeAwaiterRunnable<>(awaiter, initial, sink, initialAwaitMs, retryDelayMs);
			TRANSITION_AWAIT_POOL.submit(poller);
		}

		DisposeAwaiterRunnable(DisposeAwaiter<T> awaiter, T initial, FluxSink<Void> sink, int initialAwaitMs, int retryDelayMs) {
			this.awaiter = awaiter;
			this.initial = initial;
			this.sink = sink;
			this.initialAwaitMs = initialAwaitMs;
			this.retryDelay = retryDelayMs;
			//can only call onCancel once so we rely on DisposeAwaiterRunnable#cancel
			sink.onCancel(this::cancel);
		}

		void cancel() {
			STATE.set(this, 2);
			//we don't really care about the future. next round we'll abandon the task
		}

		@Override
		public void run() {
			if (state == 2) {
				return;
			}
			boolean awaitDone = false;
			// should be called at the beginning when STATE == 0:
			// we give the Scheduler a chance to terminate faster than the delay.
			// after that, we'll poll regularly (which means the sink can't be completed faster than increments of the delay)
			if (STATE.compareAndSet(this, 0, 1)) {
				try {
					awaitDone = awaiter.await(initial, initialAwaitMs, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					return;
				}
			}
			else if (state == 1) {
				awaitDone = awaiter.tryAwait(initial);
			}
			else {
				return;
			}

			if (awaitDone) {
				sink.complete();
			}
			else {
				if (state == 2) {
					return;
				}
				// trampoline / retry in 100ms
				TRANSITION_AWAIT_POOL.schedule(this, this.retryDelay, TimeUnit.MILLISECONDS);
			}
		}
	}
}

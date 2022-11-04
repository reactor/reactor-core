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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
				Flux.<Void>create(sink -> awaitInPool(awaiter, initial, sink, 100))
					.replay()
					.refCount()
					.next());
	}

	interface DisposeAwaiter<T> {

		boolean await(T resource, long timeout, TimeUnit timeUnit) throws InterruptedException;
	}

	static class DisposeAwaiterRunnable<T> implements Runnable {

		static final ScheduledExecutorService TRANSITION_AWAIT_POOL;

		static {
			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(0);
			executor.setKeepAliveTime(10, TimeUnit.SECONDS);
			executor.allowCoreThreadTimeOut(true);
			executor.setMaximumPoolSize(Schedulers.DEFAULT_POOL_SIZE);
			TRANSITION_AWAIT_POOL = executor;
		}

		private final DisposeAwaiter<T> awaiter;
		private final T                 initial;
		private final int               awaitMs;
		private final FluxSink<Void>    sink;

		volatile boolean cancelled;

		static <R> void awaitInPool(DisposeAwaiter<R> awaiter, R initial, FluxSink<Void> sink, int awaitMs) {
			DisposeAwaiterRunnable<R> poller = new DisposeAwaiterRunnable<>(awaiter, initial, sink, awaitMs);
			TRANSITION_AWAIT_POOL.submit(poller);
		}

		DisposeAwaiterRunnable(DisposeAwaiter<T> awaiter, T initial, FluxSink<Void> sink, int awaitMs) {
			this.awaiter = awaiter;
			this.initial = initial;
			this.sink = sink;
			this.awaitMs = awaitMs;
			//can only call onCancel once so we rely on DisposeAwaiterRunnable#cancel
			sink.onCancel(this::cancel);
		}

		void cancel() {
			cancelled = true;
			//we don't really care about the future. next round we'll abandon the task
		}

		@Override
		public void run() {
			if (cancelled) {
				return;
			}
			try {
				if (awaiter.await(initial, awaitMs, TimeUnit.MILLISECONDS)) {
					sink.complete();
				}
				else {
					if (cancelled) {
						return;
					}
					// trampoline
					TRANSITION_AWAIT_POOL.submit(this);
				}
			}
			catch (InterruptedException e) {
				//NO-OP
			}
		}
	}
}

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

import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

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
						Flux.<Void>create(sink -> {
									// TODO(dj): consider a shared pool for all disposeGracefully background tasks
									// as part of Schedulers internal API
									Thread backgroundThread = new Thread(() -> {
										while (!Thread.currentThread().isInterrupted()) {
											try {
												if (awaiter.await(initial, 1, TimeUnit.SECONDS)) {
													sink.complete();
													return;
												}
											} catch (InterruptedException e) {
												return;
											}
										}
									});
									sink.onCancel(backgroundThread::interrupt);
									backgroundThread.start();
								})
								.replay()
								.refCount()
								.next());
	}

	interface DisposeAwaiter<T> {
		boolean await(T resource, long timeout, TimeUnit timeUnit) throws InterruptedException;
	}
}

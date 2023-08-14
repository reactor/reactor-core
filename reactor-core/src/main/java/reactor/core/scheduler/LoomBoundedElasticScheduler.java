/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.Disposable;
import reactor.core.Scannable;

final class LoomBoundedElasticScheduler
		implements Scheduler, SchedulerState.DisposeAwaiter<LoomBoundedElasticScheduler.BoundedServices>, Scannable {

	LoomBoundedElasticScheduler(int maxThreads, int maxTaskQueuedPerThread, ThreadFactory threadFactory, long ttlMillis, Clock clock) {
		throw new UnsupportedOperationException("Unsupported in JDK lower thank 21");
	}

	/**
	 * Create a {@link BoundedElasticScheduler} with the given configuration. Note that backing threads
	 * (or executors) can be shared by each {@link reactor.core.scheduler.Scheduler.Worker}, so each worker
	 * can contribute to the task queue size.
	 *
	 * @param maxThreads             the maximum number of backing threads to spawn, must be strictly positive
	 * @param maxTaskQueuedPerThread the maximum amount of tasks an executor can queue up
	 * @param factory                the {@link ThreadFactory} to name the backing threads
	 * @param ttlSeconds             the time-to-live (TTL) of idle threads, in seconds
	 */
	LoomBoundedElasticScheduler(int maxThreads, int maxTaskQueuedPerThread, ThreadFactory factory, int ttlSeconds) {
		throw new UnsupportedOperationException("Unsupported in JDK lower thank 21");
	}

	@Override
	public boolean await(BoundedServices resource, long timeout, TimeUnit timeUnit)
			throws InterruptedException {
		return false;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null;
	}

	@Override
	public Disposable schedule(Runnable task) {
		throw new UnsupportedOperationException("Unsupported in JDK lower thank 21");
	}

	@Override
	public Worker createWorker() {
		throw new UnsupportedOperationException("Unsupported in JDK lower thank 21");
	}

	static final class BoundedServices extends AtomicInteger {

	}
}
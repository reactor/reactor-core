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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import reactor.core.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class ExecutorSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Override
	protected boolean shouldCheckDirectTimeScheduling() {
		return false;
	}

	@Override
	protected boolean shouldCheckWorkerTimeScheduling() {
		return false;
	}

	@Override
	protected Scheduler scheduler() {
		return Schedulers.fromExecutor(Runnable::run);
	}

	@Test
	public void directAndWorkerTimeSchedulingRejected() {
		Scheduler scheduler = scheduler();
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> scheduler.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> scheduler.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());

			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> worker.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> worker.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
		}
		finally {
			worker.dispose();
		}
	}

	@Test
	public void failingExecutorRejects() {
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorScheduler scheduler = new ExecutorScheduler(
				task -> { throw boom;},
				false
		);

		AtomicBoolean done = new AtomicBoolean();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> scheduler.schedule(() -> done.set(true)))
				.withCause(boom);

		assertThat(done.get()).isFalse();
	}

	@Test
	public void failingExecutorIsTerminated() {
		AtomicInteger count = new AtomicInteger();
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorScheduler scheduler = new ExecutorScheduler(
				task -> {
					if (count.incrementAndGet() == 1)
						throw boom;
					},
				false
		);

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.withCause(boom);

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.withNoCause()
				.isSameAs(Exceptions.failWithRejected());

		assertThat(count.get()).isEqualTo(1);
	}
}

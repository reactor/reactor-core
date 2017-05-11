/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.assertj.core.data.Offset;
import org.junit.Test;
import reactor.core.Disposable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class DefaultFunctionalSchedulerTest {

	private static class TestDisposable implements Disposable {

		final Runnable task;
		final long workerId;

		private TestDisposable(Runnable task, long id) {
			this.task = task;
			this.workerId = id;
		}

		public Runnable getTask() {
			return task;
		}

		public long getWorkerId() {
			return workerId;
		}

		@Override
		public void dispose() {
			//NO-OP
		}
	}

	static long workerCount = 0L;

	Scheduler scheduler = () -> {
		final long workerId = workerCount++;
		return task -> new TestDisposable(task, workerId);
	};

	Scheduler.Worker worker = scheduler.createWorker();

	@Test
	public void defaultScheduleDelegatesToNewWorker() {
		long beforeId = workerCount;
		scheduler.schedule(() -> {});
		assertThat(workerCount).isGreaterThan(beforeId);
	}

	@Test
	public void defaultScheduleTimedDelegatesToNewWorker() {
		long beforeId = workerCount;
		scheduler.schedule(() -> {}, 10, TimeUnit.MILLISECONDS);
		assertThat(workerCount).isGreaterThan(beforeId);
	}

	@Test
	public void defaultSchedulePeriodicallyDelegatesToNewWorker() {
		long beforeId = workerCount;
		scheduler.schedulePeriodically(() -> {}, 10, 10, TimeUnit.MILLISECONDS);
		assertThat(workerCount).isGreaterThan(beforeId);
	}

	@Test
	public void defaultDisposeIsNoOp() {
		scheduler.dispose();

		assertThat(scheduler.isDisposed()).isFalse();
		assertThat(scheduler.schedule(() -> {})).isNotSameAs(Scheduler.REJECTED);
	}

	@Test
	public void defaultNowIsSystemClock() throws InterruptedException {
		long now = System.currentTimeMillis();
		assertThat(scheduler.now(TimeUnit.MILLISECONDS)).isCloseTo(now, Offset.offset(10L));
	}

	@Test
	public void defaultWorkerSchedules() {
		Runnable task = () -> {};
		assertThat(worker.schedule(task))
				.isInstanceOfSatisfying(TestDisposable.class,
						td -> assertThat(td.getTask()).isSameAs(task));
	}

	@Test
	public void defaultWorkerTimeRejects() {
		Runnable task = () -> {};
		assertThat(worker.schedule(task, 10, TimeUnit.MILLISECONDS))
				.isSameAs(Scheduler.REJECTED);
	}

	@Test
	public void defaultWorkerPeriodicallyRejects() {
		Runnable task = () -> {};
		assertThat(worker.schedulePeriodically(task, 10, 10, TimeUnit.MILLISECONDS))
				.isSameAs(Scheduler.REJECTED);
	}

	@Test
	public void defaultWorkerDisposeIsNoOp() {
		worker.dispose();
		assertThat(worker.isDisposed()).isFalse();
		assertThat(worker.schedule(() -> {})).isNotSameAs(Scheduler.REJECTED);
	}

}
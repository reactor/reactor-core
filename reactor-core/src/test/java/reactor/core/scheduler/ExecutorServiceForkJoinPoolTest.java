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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.scheduler.Scheduler.Worker;

import static org.assertj.core.api.Assertions.assertThat;

public class ExecutorServiceForkJoinPoolTest extends AbstractSchedulerTest {

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
		return Schedulers.fromExecutor(new ForkJoinPool());
	}

	@Test
	public void notScheduledRejects() {
		Scheduler s = scheduler();
		assertThat(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isSameAs(Scheduler.REJECTED);
		assertThat(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isSameAs(Scheduler.REJECTED);

		Worker w = s.createWorker();
		assertThat(w.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("worker delayed scheduling")
				.isSameAs(Scheduler.REJECTED);
		assertThat(w.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("worder periodic scheduling")
				.isSameAs(Scheduler.REJECTED);
	}

	@Test
	public void directAndWorkerTimeSchedulingRejected() {
		Scheduler scheduler = scheduler();
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			assertThat(scheduler.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Scheduler.REJECTED);
			assertThat(scheduler.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Scheduler.REJECTED);

			assertThat(worker.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Scheduler.REJECTED);
			assertThat(worker.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Scheduler.REJECTED);
		}
		finally {
			worker.dispose();
		}
	}
}

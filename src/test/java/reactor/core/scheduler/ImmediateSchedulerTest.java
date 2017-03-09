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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.scheduler.Scheduler.Worker;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ImmediateSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.immediate();
	}

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Override
	protected boolean shouldCheckMassWorkerDispose() {
		return false;
	}

	@Test
	public void directAndWorkerTimeSchedulingRejected() {
		Scheduler scheduler = scheduler();
		Worker worker = scheduler.createWorker();
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

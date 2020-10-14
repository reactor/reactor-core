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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class SingleWorkerSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.single(Schedulers.immediate());
	}

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
	protected boolean shouldCheckSupportRestart() {
		return false;
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = Schedulers.single(Schedulers.newSingle("scanName"));
		Scheduler withBasicFactory = Schedulers.single(Schedulers.newParallel(9, Thread::new));

		Scheduler.Worker workerWithNamedFactory = withNamedFactory.createWorker();
		Scheduler.Worker workerWithBasicFactory = withBasicFactory.createWorker();

		try {
			assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
					.as("withNamedFactory")
					.isEqualTo("singleWorker(ExecutorServiceWorker)");

			assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
					.as("withBasicFactory")
					.isEqualTo("singleWorker(ExecutorServiceWorker)");

			assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
					.as("workerWithNamedFactory")
					.isEqualTo("singleWorker(ExecutorServiceWorker).worker");

			assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
					.as("workerWithBasicFactory")
					.isEqualTo("singleWorker(ExecutorServiceWorker).worker");
		}
		finally {
			withNamedFactory.dispose();
			withBasicFactory.dispose();
			workerWithNamedFactory.dispose();
			workerWithBasicFactory.dispose();
		}
	}
}

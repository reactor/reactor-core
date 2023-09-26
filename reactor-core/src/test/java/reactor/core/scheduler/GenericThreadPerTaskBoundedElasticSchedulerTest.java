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

import org.junit.jupiter.api.Assertions;

class GenericThreadPerTaskBoundedElasticSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		ThreadPerTaskBoundedElasticScheduler test =
				new ThreadPerTaskBoundedElasticScheduler(4,
						Integer.MAX_VALUE,
						new VirtualThreadFactory(
								"threadPerTaskBoundedElasticSchedulerTest", false,
								Schedulers::defaultUncaughtException));
		test.init();
		return test;
	}

	@Override
	protected Scheduler freshScheduler() {
		return new ThreadPerTaskBoundedElasticScheduler(4,
				Integer.MAX_VALUE,
				new VirtualThreadFactory(
						"threadPerTaskBoundedElasticSchedulerTest", false,
						Schedulers::defaultUncaughtException));
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Override
	protected boolean shouldCheckMultipleDisposeGracefully() {
		return true;
	}

	@Override
	public void acceptTaskAfterStartStopStart() {
		Assertions.fail("no restart supported");
	}

	@Override
	public void restartSupport() {
		Assertions.fail("no restart supported");
	}

	@Override
	void multipleRestarts() {
		Assertions.fail("no restart supported");
	}
}
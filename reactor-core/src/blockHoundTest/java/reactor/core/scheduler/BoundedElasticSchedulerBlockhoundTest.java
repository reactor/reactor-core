/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.concurrent.FutureTask;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.Disposable;

public class BoundedElasticSchedulerBlockhoundTest {

	@BeforeAll
	public static void setup() {
		BlockHound.install();
	}

	//see https://github.com/reactor/reactor-core/issues/2143
	@Test
	public void shouldNotReportBlockingCallWithZoneIdUsage() throws Throwable {
		FutureTask<Disposable> testTask = new FutureTask<>(() -> new BoundedElasticScheduler(1, 1, Thread::new, 1));
		Schedulers.single().schedule(testTask);
		//automatically re-throw in case of blocking call, in which case the scheduler hasn't been created so there is no leak.
		testTask.get().dispose();
	}

}

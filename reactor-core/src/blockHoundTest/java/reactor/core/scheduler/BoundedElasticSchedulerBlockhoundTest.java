/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;

public class BoundedElasticSchedulerBlockhoundTest {

	private static final Logger     LOGGER  = Loggers.getLogger(BoundedElasticSchedulerBlockhoundTest.class);

	private Disposable.Composite toDispose;

	@BeforeAll
	static void setup() {
		BlockHound.install();
	}

	@BeforeEach
	void setupComposite() {
		toDispose = Disposables.composite();
	}

	@AfterEach
	void dispose() {
		toDispose.dispose();
	}

	<T extends Disposable> T autoDispose(T disposable) {
		toDispose.add(disposable);
		return disposable;
	}

	@Test
	void smokeTestBlockhound() throws InterruptedException, TimeoutException {
		try {
			FutureTask<?> task = new FutureTask<>(() -> {
				Thread.sleep(0);
				return "";
			});
			Schedulers.parallel().schedule(task);

			task.get(10, TimeUnit.SECONDS);
			Assertions.fail("should fail");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(BlockingOperationError.class);
		}
	}

	//see https://github.com/reactor/reactor-core/issues/2143
	@Test
	void shouldNotReportBlockingCallWithZoneIdUsage() throws Throwable {
		FutureTask<Disposable> testTask = new FutureTask<>(() -> new BoundedElasticScheduler(1, 1, Thread::new, 1));
		Schedulers.single().schedule(testTask);
		//automatically re-throw in case of blocking call, in which case the scheduler hasn't been created so there is no leak.
		testTask.get().dispose();
	}

	@RepeatedTest(3) //we got false positives from time to time. with repeat(3), only 1 block out of 500 was a false positive (with a total of 75 false positives out of 1500 runs)
	void shouldNotReportEnsureQueueCapacity() {
		BoundedElasticScheduler scheduler = autoDispose(new BoundedElasticScheduler(1, 100, Thread::new, 1));
		scheduler.init();
		ExecutorServiceWorker worker = (ExecutorServiceWorker) autoDispose(scheduler.createWorker());
		BoundedElasticScheduler.BoundedScheduledExecutorService executor =
			(BoundedElasticScheduler.BoundedScheduledExecutorService) worker.exec;

		AtomicReference<Throwable> error = new AtomicReference<>();
		Runnable runnable = () -> {
			try {
				executor.ensureQueueCapacity(1);
			}
			catch (final Throwable t) {
				error.updateAndGet(current -> {
					if (current == null) return t;
					List<Throwable> multiple = new ArrayList<>(Exceptions.unwrapMultiple(current));
					multiple.add(t);
					return Exceptions.multiple(multiple);
				});
			}
		};

		Scheduler sch = autoDispose(Schedulers.newParallel("test", 10));

		RaceTestUtils.race(sch,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable,
			runnable);

		//assertions don't really show the stack trace unless we modify a global config
		//so here we simply throw the composite if there is one
		if (error.get() != null) throw Exceptions.propagate(error.get());
	}
}

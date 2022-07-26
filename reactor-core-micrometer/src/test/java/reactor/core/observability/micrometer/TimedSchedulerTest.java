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

package reactor.core.observability.micrometer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import org.assertj.core.api.SoftAssertionsProvider;
import org.assertj.core.data.Offset;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class TimedSchedulerTest {

	@RegisterExtension
	AutoDisposingExtension afterTest = new AutoDisposingExtension();
	private SimpleMeterRegistry registry;

	@BeforeEach
	void setUp() {
		registry = new SimpleMeterRegistry();
	}

	@Test
	void constructorAddsDotToPrefixIfNeeded() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "noDot", Tags.empty());

		assertThat(test.executionTimer.getId().getName()).startsWith("noDot.");
		assertThat(test.scheduledOnce.getId().getName()).startsWith("noDot.");
		assertThat(test.scheduledPeriodically.getId().getName()).startsWith("noDot.");
		assertThat(test.idleTimer.getId().getName()).startsWith("noDot.");
	}

	@Test
	void constructorDoesntAddTwoDots() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "dot.", Tags.empty());

		assertThat(test.executionTimer.getId().getName()).doesNotContain("..");
		assertThat(test.scheduledOnce.getId().getName()).doesNotContain("..");
		assertThat(test.scheduledPeriodically.getId().getName()).doesNotContain("..");
		assertThat(test.idleTimer.getId().getName()).doesNotContain("..");
	}

	@Test
	void constructorRegistersFourMeters() {
		MeterRegistryAssert.assertThat(registry).as("before constructor").hasNoMetrics();

		new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		MeterRegistryAssert.assertThat(registry)
				.hasMeterWithName("test.scheduler.idle")
				.hasMeterWithName("test.scheduler") //FIXME better meter name?
				.hasMeterWithName("test.scheduler.scheduled.once")
				.hasMeterWithName("test.scheduler.scheduled.periodically");
	}

	@Test
	void scheduleWhileBusyIncrementsIdleTime() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		final CountDownLatch firstTaskPause = new CountDownLatch(1);
		final CountDownLatch secondTaskDone = new CountDownLatch(1);
		test.schedule(() -> {
			System.out.println("start task1");
			try {
				firstTaskPause.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
		test.schedule(() -> {
			System.out.println("start task2");
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			finally {
				secondTaskDone.countDown();
			}
		});

		//FIXME I'm not measuring what I think I'm measuring

		assertThat(test.executionTimer.totalTime(TimeUnit.MILLISECONDS))
			.as("initial execution totalTime")
			.isZero();
		assertThat(test.idleTimer.totalTime(TimeUnit.MILLISECONDS))
			.as("initial idle totalTime")
			.isCloseTo(0d, Offset.offset(50d));
		//FIXME the timers don't register new tasks when sampling starts, so count doesn't really represent current submitted / pending tasks
//		assertThat(test.executionTimer.count()).as("one active").isOne();
//		assertThat(test.idleTimer.count()).as("one idle").isOne();

		Thread.sleep(2000);
		firstTaskPause.countDown();

		assertThat(test.executionTimer.totalTime(TimeUnit.SECONDS)).as("active totalTime SECONDS").isEqualTo(2);
		assertThat(test.idleTimer.totalTime(TimeUnit.SECONDS)).as("idle totalTime SECONDS").isEqualTo(2);

		secondTaskDone.countDown();

		assertThat(test.executionTimer.totalTime(TimeUnit.SECONDS)).as("active totalTime SECONDS").isEqualTo(4);
		assertThat(test.idleTimer.totalTime(TimeUnit.SECONDS)).as("idle totalTime SECONDS").isEqualTo(2);
	}

	@Test
	void scheduleDoesntIncrementCounterButAddsToTimerCount() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		test.schedule(() -> {});

		assertThat(test.executionTimer.count()).as("executionTimer.count").isOne();
		assertThat(test.scheduledOnce.count()).as("scheduledOnce").isZero();
		assertThat(test.scheduledPeriodically.count()).as("scheduledPeriodically").isZero();
	}

	@Test
	void scheduleDelayIncrementsOnceCounter() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		CountDownLatch latch = new CountDownLatch(1);

		test.schedule(latch::countDown, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		assertThat(test.executionTimer.count()).as("executionTimer.count").isOne();
		assertThat(test.scheduledOnce.count()).as("scheduledOnce").isOne();
		assertThat(test.scheduledPeriodically.count()).as("scheduledPeriodically").isZero();
	}

	@Test
	void schedulePeriodicallyIncrements_ExecutionCountUntilDisposed_PeriodicallyCounterOnce() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		Disposable d = test.schedulePeriodically(latch::countDown,100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(test.executionTimer.count()).as("executionTimer.count").isEqualTo(5);
		assertThat(test.scheduledOnce.count()).as("scheduledOnce").isZero();
		assertThat(test.scheduledPeriodically.count()).as("scheduledPeriodically").isOne();
	}

	@Test
	void createWorkerDelegatesToAnOriginalWorker() {
		Scheduler mockScheduler = Mockito.mock(Scheduler.class);
		Scheduler.Worker mockWorker = Mockito.spy(Scheduler.Worker.class);
		Mockito.when(mockScheduler.createWorker()).thenReturn(mockWorker);

		TimedScheduler test = new TimedScheduler(mockScheduler, registry, "test", Tags.empty());

		TimedScheduler.TimedWorker worker = (TimedScheduler.TimedWorker) test.createWorker();

		assertThat(worker.delegate).as("worker delegate").isSameAs(mockWorker);
	}

	@Test
	void workerScheduleDoesntIncrementCounterButAddsToIdleCount() {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		test.schedule(() -> {});

		assertThat(testScheduler.executionTimer.count()).as("executionTimer.count").isOne();
		assertThat(testScheduler.scheduledOnce.count()).as("worker scheduledOnce").isZero();
		assertThat(testScheduler.scheduledPeriodically.count()).as("worker scheduledPeriodically").isZero();
	}

	@Test
	void workerScheduleDelayIncrementsOnceCounter() throws InterruptedException {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();
		CountDownLatch latch = new CountDownLatch(1);

		test.schedule(latch::countDown, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		assertThat(testScheduler.executionTimer.count()).as("executionTimer.count").isOne();
		assertThat(testScheduler.scheduledOnce.count()).as("worker scheduledOnce").isOne();
		assertThat(testScheduler.scheduledPeriodically.count()).as("worker scheduledPeriodically").isZero();
	}

	@Test
	void workerSchedulePeriodicallyIncrements_ExecutionCountUntilDisposed_PeriodicallyCounterOnce() throws InterruptedException {
		Scheduler original = afterTest.autoDispose(Schedulers.single());
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler testScheduler = new TimedScheduler(original, registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		Disposable d = test.schedulePeriodically(latch::countDown,100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(testScheduler.executionTimer.count()).as("executionTimer.count").isEqualTo(5);
		assertThat(testScheduler.scheduledOnce.count()).as("worker scheduledOnce").isZero();
		assertThat(testScheduler.scheduledPeriodically.count()).as("worker scheduledPeriodically").isOne();
	}
}
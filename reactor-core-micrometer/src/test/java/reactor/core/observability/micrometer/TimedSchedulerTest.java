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
import org.awaitility.Awaitility;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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

		assertThat(registry.getMeters())
			.map(m -> m.getId().getName())
			.allSatisfy(name -> assertThat(name).startsWith("noDot."));
	}

	@Test
	void constructorDoesntAddTwoDots() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "dot.", Tags.empty());

		assertThat(registry.getMeters())
			.map(m -> m.getId().getName())
			.allSatisfy(name -> assertThat(name).doesNotContain(".."));
	}

	@Test
	void constructorRegistersSixMeters() {
		MeterRegistryAssert.assertThat(registry).as("before constructor").hasNoMetrics();

		new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		assertThat(registry.getMeters())
			.map(m -> m.getId().getName())
			.containsExactlyInAnyOrder(
				"test.scheduler.tasks.active",
				"test.scheduler.tasks.completed",
				"test.scheduler.tasks.pending",
				"test.scheduler.tasks.submitted",
				"test.scheduler.tasks.submitted.delayed",
				"test.scheduler.tasks.submitted.periodically"
			);
	}

	@Test
	void scheduleWhileBusyAddsToPendingTime() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		/*
		This test schedules two tasks in a Schedulers.single(), using latches to "pause" and "resume" the
		tasks at points where we can make predictable assertions. As a result, task2 will be pending until
		task1 is un-paused.

		LongTaskTimer only report timings and counts of Runnable that are being executed. Once the Runnable
		finishes, LTT won't report any activity.

		Timer on the other hand will report cumulative times and counts AFTER the Runnable has finished.
		This is used for the completedTasks metric, which is asserted at the end.
		 */

		final CountDownLatch firstTaskPause = new CountDownLatch(1);
		final CountDownLatch secondTaskDone = new CountDownLatch(1);
		test.schedule(() -> {
			try {
				firstTaskPause.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
		test.schedule(() -> {
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			finally {
				secondTaskDone.countDown();
			}
		});

		//there might be a slight hiccup when the registry doesn't see task1 as active
		Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(
			() -> assertThat(test.activeTasks.activeTasks()).as("one active").isOne());
		assertThat(test.pendingTasks.activeTasks()).as("one idle").isOne();

		//we give it 2s, expecting that pendingTasks and activeTasks both reflect these 2 seconds (for task2 and task1 respectively)
		Thread.sleep(2000);

		assertThat(Math.round(test.pendingTasks.duration(TimeUnit.SECONDS)))
			.as("after 1st idle totalTime SECONDS")
			.isEqualTo(2);
		assertThat(Math.round(test.activeTasks.duration(TimeUnit.SECONDS)))
			.as("after 1st active totalTime SECONDS")
			.isEqualTo(2);

		// we "resume" both tasks and let them finish, at which point the LongTaskTimers will stop recording
		firstTaskPause.countDown();
		secondTaskDone.await(10, TimeUnit.SECONDS);

		//again, there might be a slight hiccup before registry sees 2nd task as done
		Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(
			() -> assertThat(test.activeTasks.duration(TimeUnit.SECONDS))
			.as("once 2nd done, no active timing")
			.isEqualTo(0));
		assertThat(test.pendingTasks.duration(TimeUnit.SECONDS))
			.as("once 2nd done, no pending timing")
			.isEqualTo(0);
		assertThat(test.pendingTasks.activeTasks()).as("at end pendingTasks").isZero();
		assertThat(test.activeTasks.activeTasks()).as("at end activeTasks").isZero();

		//now we assert that the completedTasks timer reflects an history of all Runnable#run
		assertThat(test.completedTasks.count())
			.as("#completed")
			.isEqualTo(2L);
		assertThat(test.completedTasks.totalTime(TimeUnit.MILLISECONDS))
			.as("total duration of tasks")
			.isCloseTo(2500, Offset.offset(200d));
	}


	@Test
	void schedulePeriodicallyTimesOneRunInActiveAndAllRunsInCompleted() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		//schedule a periodic task for which one run takes 500ms. we cancel after 3 runs
		CountDownLatch latch = new CountDownLatch(3);
		Disposable d = test.schedulePeriodically(
			() -> {
				try {
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					latch.countDown();
				}
			},
			100, 100, TimeUnit.MILLISECONDS);
		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		//now we assert that the completedTasks timer reflects an history of all Runnable#run
		assertThat(test.submittedTotal.count()).as("#submittedTotal").isOne();
		assertThat(test.submittedPeriodically.count()).as("#submittedPeriodically").isOne();
		assertThat(test.completedTasks.count())
			.as("#completed")
			.isEqualTo(3L);
		assertThat(test.completedTasks.totalTime(TimeUnit.MILLISECONDS))
			.as("total duration of tasks")
			.isCloseTo(1500, Offset.offset(200d));
	}

	@Test
	void scheduleIncrementTotalCounterOnly() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		test.schedule(() -> {});

		assertThat(test.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(test.submittedPeriodically.count()).as("submittedPeriodically.count").isZero();
	}

	@Test
	void scheduleDelayIncrementsTotalAndDelayedCounters() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		test.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);

		assertThat(test.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isOne();
		assertThat(test.submittedPeriodically.count()).as("submittedPeriodically.count").isZero();
	}

	@Test
	void schedulePeriodicallyIncrementsTotalAndPeriodicallyCountersByOne() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		Disposable d = test.schedulePeriodically(latch::countDown,100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(test.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(test.submittedPeriodically.count()).as("submittedPeriodically.count").isOne();

		assertThat(test.completedTasks.count()).as("completed counter tracks all iterations").isEqualTo(5);
	}

	@Test
	void schedulePeriodicallyIncrementsTotalRunsByNumberOfIterations() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		Disposable d = test.schedulePeriodically(latch::countDown,100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(test.completedTasks.count()).isEqualTo(5);
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
	void workerScheduleIncrementsTotalCounterOnly() {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		test.schedule(() -> {});

		assertThat(testScheduler.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(testScheduler.submittedPeriodically.count()).as("submittedPeriodically.count").isZero();
	}

	@Test
	void workerScheduleDelayIncrementsTotalAndDelayedCounters() throws InterruptedException {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		test.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);

		assertThat(testScheduler.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isOne();
		assertThat(testScheduler.submittedPeriodically.count()).as("submittedPeriodically.count").isZero();
	}

	@Test
	void workerSchedulePeriodicallyIncrementsTotalAndPeriodicallyCountersByOne() throws InterruptedException {
		Scheduler original = afterTest.autoDispose(Schedulers.single());
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler testScheduler = new TimedScheduler(original, registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		Disposable d = test.schedulePeriodically(latch::countDown,100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(testScheduler.submittedTotal.count()).as("submittedTotal.count").isOne();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(testScheduler.submittedPeriodically.count()).as("submittedPeriodically.count").isOne();

		assertThat(testScheduler.completedTasks.count()).as("completed counter tracks all iterations").isEqualTo(5);
	}
}
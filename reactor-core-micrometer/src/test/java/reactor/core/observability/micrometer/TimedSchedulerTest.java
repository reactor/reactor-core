/*
 * Copyright (c) 2022-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;

import static org.assertj.core.api.Assertions.*;

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

	@AfterEach
	void closeRegistry() {
		registry.close();
	}

	@Test
	void supportsBothDeprecatedAndNonRestartableSchedulers() {
		Scheduler deprecatedScheduler = new Scheduler() {

			@Override
			public Disposable schedule(Runnable task) {
				return Disposables.disposed();
			}

			@Override
			public Worker createWorker() {
				throw new UnsupportedOperationException();
			}
		};

		Scheduler nonRestartableScheduler = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task) {
				return Disposables.disposed();
			}

			@Override
			public Worker createWorker() {
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("deprecation")
			@Override
			public void start() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void init() {
			}
		};

		TimedScheduler timedDeprecatedScheduler =
				new TimedScheduler(deprecatedScheduler, registry, "test", Tags.empty());

		TimedScheduler timedNonRestartableScheduler =
				new TimedScheduler(nonRestartableScheduler, registry, "test", Tags.empty());

		assertThatNoException().isThrownBy(() -> {
			timedDeprecatedScheduler.init();
			timedDeprecatedScheduler.start();
		});

		assertThatNoException().isThrownBy(timedNonRestartableScheduler::init);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(timedNonRestartableScheduler::start);
	}

	@Test
	void aDotIsAddedToPrefix() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "noDot", Tags.empty());

		assertThat(registry.getMeters())
			.map(m -> m.getId().getName())
			.isNotEmpty()
			.allSatisfy(name -> assertThat(name).startsWith("noDot."));
	}

	@Test
	@Tag("slow")
	// see https://github.com/reactor/reactor-core/issues/3844
	void cancellationClearsPendingTasks() throws Exception {
		TimedScheduler scheduler = (TimedScheduler) Micrometer.timedScheduler(
				Schedulers.newSingle("single-test-scheduler"),
				new SimpleMeterRegistry(),
				"test");

		AtomicLong totalCalls = new AtomicLong();
		AtomicLong errorCalls = new AtomicLong();

		int iterations = 500_000;

		new Thread(() -> {
			for (int i = 0; i < iterations; i++) {
				Mono.delay(Duration.ofMillis(5))
						.subscribeOn(scheduler)
						.timeout(Duration.ofMillis(20), scheduler)
						.subscribe(
								__ -> totalCalls.incrementAndGet(),
								__ -> {
									totalCalls.incrementAndGet();
									errorCalls.incrementAndGet();
								});
			}
		}).start();

		while (totalCalls.get() < iterations) {
			Thread.sleep(100);
			System.out.printf("Progress: %.1f\n", 100d / iterations * totalCalls.get());
		}

		scheduler.dispose();

		System.out.println("Pending tasks: " + scheduler.pendingTasks.activeTasks());
		System.out.println("Error calls: " + errorCalls.get());

		assertThat(scheduler.pendingTasks.activeTasks()).isEqualTo(0);
	}

	@Test
	// see https://github.com/reactor/reactor-core/issues/3844
	void disposeClearsPendingTasksInWorker() throws Exception {
		TimedScheduler scheduler = (TimedScheduler) Micrometer.timedScheduler(
				Schedulers.newSingle("ttt"),
				new SimpleMeterRegistry(),
				"test");

		TimedScheduler.TimedWorker worker = (TimedScheduler.TimedWorker) scheduler.createWorker();

		worker.schedule(() -> {
			try {
				System.out.println("First task run");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("First task interrupted");
			}
		});

		worker.schedule(() -> {
			try {
				System.out.println("Second task run");
				Thread.sleep(500);
			} catch (InterruptedException e) {
				System.out.println("Second task interrupted");
			}
		});

		worker.dispose();

		assertThat(scheduler.pendingTasks.activeTasks()).isEqualTo(0);
	}

	@Test
	void constructorIgnoresDotAtEndOfMetricPrefix() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "dot.", Tags.empty());

		assertThat(registry.getMeters())
			.map(m -> m.getId().getName())
			.isNotEmpty()
			.allSatisfy(name -> assertThat(name)
				.startsWith("dot.")
				.doesNotContain(".."));
	}

	@Test
	void constructorRegistersSevenMetersWithFourSimilarCountersWithSubmissionTypeTag() {
		MeterRegistryAssert.assertThat(registry).as("before constructor").hasNoMetrics();

		new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		assertThat(registry.getMeters())
			.map(m -> {
				String name = m.getId().getName();
				String type = m.getId().getTag("submission.type");
				return name + (type == null ? "" : " submission.type=" + type);
			})
			.containsExactlyInAnyOrder(
				"test.scheduler.tasks.active",
				"test.scheduler.tasks.completed",
				"test.scheduler.tasks.pending",
				//technically 4 different submitted counters
				"test.scheduler.tasks.submitted submission.type=direct",
				"test.scheduler.tasks.submitted submission.type=delayed",
				"test.scheduler.tasks.submitted submission.type=periodic_initial",
				"test.scheduler.tasks.submitted submission.type=periodic_iteration"
			);
	}

	@Test
	void timingOfActiveAndPendingTasks() throws InterruptedException {
		MockClock virtualClock = new MockClock();
		SimpleMeterRegistry registryWithVirtualClock = new SimpleMeterRegistry(SimpleConfig.DEFAULT, virtualClock);
		afterTest.autoDispose(registryWithVirtualClock::close);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registryWithVirtualClock, "test", Tags.empty());

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
				firstTaskPause.await(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
		test.schedule(() -> {
			try {
				virtualClock.addSeconds(1);
			}
			finally {
				secondTaskDone.countDown();
			}
		});

		//there might be a slight hiccup when the registry doesn't see task1 as active
		Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(
			() -> assertThat(test.activeTasks.activeTasks()).as("one active").isOne());
		assertThat(test.pendingTasks.activeTasks()).as("one idle").isOne();

		//we advance time by 2s, expecting that pendingTasks and activeTasks both reflect these 2 seconds (for task2 and task1 respectively)
		virtualClock.addSeconds(2);

		assertThat(test.pendingTasks.duration(TimeUnit.SECONDS))
			.as("after 1st idle totalTime SECONDS")
			.isEqualTo(2);
		assertThat(test.activeTasks.duration(TimeUnit.SECONDS))
			.as("after 1st active totalTime SECONDS")
			.isEqualTo(2);

		// we "resume" both tasks and let them finish, at which point the LongTaskTimers will stop recording
		firstTaskPause.countDown();
		secondTaskDone.await(1, TimeUnit.SECONDS);

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

		//now we assert that the completedTasks timer reflects a history of all Runnable#run
		assertThat(test.completedTasks.count())
			.as("#completed")
			.isEqualTo(2L);
		assertThat(test.completedTasks.totalTime(TimeUnit.MILLISECONDS))
			.as("total duration of tasks")
			.isEqualTo(3000);
	}

	@Test
	void schedulePeriodicallyTimesOneRunInActiveAndAllRunsInCompleted() throws InterruptedException {
		MockClock virtualClock = new MockClock();
		SimpleMeterRegistry registryWithVirtualClock = new SimpleMeterRegistry(SimpleConfig.DEFAULT, virtualClock);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registryWithVirtualClock, "test",
				Tags.empty());

		//schedule a periodic task for which one run takes 500ms. we cancel after 3 runs
		CountDownLatch latch = new CountDownLatch(3);

		//decrement latch after all task & wrapper actions are performed
		Schedulers.onScheduleHook("test", task -> () -> {
			try {
				task.run();
			}
			finally {
				latch.countDown();
			}
		});

		Disposable d = test.schedulePeriodically(() -> virtualClock.add(Duration.ofMillis(500)),
				100, 100, TimeUnit.MILLISECONDS);

		latch.await(1, TimeUnit.SECONDS);
		d.dispose();

		//now we assert that the completedTasks timer reflects a history of all Runnable#run
		assertThat(test.submittedDirect.count()).as("#submittedDirect").isZero();
		assertThat(test.submittedPeriodicInitial.count()).as("#submittedPeriodicInitial").isOne();
		assertThat(test.submittedPeriodicIteration.count()).as("#submittedPeriodicIteration").isEqualTo(2);
		assertThat(test.completedTasks.count())
			.as("#completed")
			.isEqualTo(3L);
		assertThat(test.completedTasks.totalTime(TimeUnit.MILLISECONDS))
			.as("total duration of tasks")
			.isEqualTo(1500);

		Schedulers.resetOnScheduleHook("test");
		test.disposeGracefully().block(Duration.ofSeconds(1));
	}

	@Test
	void scheduleIncrementDirectCounterOnly() {
		TimedScheduler test = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());

		test.schedule(() -> {});

		assertThat(test.submittedDirect.count()).as("submittedDirect.count").isOne();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(test.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isZero();
		assertThat(test.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isZero();
	}

	@Test
	void scheduleDelayIncrementsDelayedCounter() throws InterruptedException {
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		test.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);

		assertThat(test.submittedDirect.count()).as("submittedDirect.count").isZero();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isOne();
		assertThat(test.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isZero();
		assertThat(test.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isZero();
	}

	@Test
	void schedulePeriodicallyIsCorrectlyMetered() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler test = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());

		Schedulers.onScheduleHook("test", task -> () -> {
			try {
				task.run();
			}
			finally {
				latch.countDown();
			}
		});

		Disposable d = test.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(test.submittedDirect.count()).as("submittedDirect.count").isZero();
		assertThat(test.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(test.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isOne();
		assertThat(test.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isEqualTo(4);
		assertThat(test.completedTasks.count())
			.as("completed counter tracks all iterations")
			.isEqualTo(5)
			.matches(l -> l == test.submittedDirect.count() + test.submittedDelayed.count()  + test.submittedPeriodicInitial.count()
				+ test.submittedPeriodicIteration.count(), "completed tasks == sum of all timer counts");

		Schedulers.resetOnScheduleHook("test");
		test.disposeGracefully().block(Duration.ofSeconds(1));
	}

	@Test
	void createWorkerDelegatesToAnOriginalWorker() {
		Scheduler mockScheduler = Mockito.mock(Scheduler.class);
		Scheduler.Worker mockWorker = Mockito.mock(Scheduler.Worker.class);
		Mockito.when(mockScheduler.createWorker()).thenReturn(mockWorker);

		TimedScheduler test = new TimedScheduler(mockScheduler, registry, "test", Tags.empty());

		TimedScheduler.TimedWorker worker = (TimedScheduler.TimedWorker) test.createWorker();

		assertThat(worker.delegate).as("worker delegate").isSameAs(mockWorker);
	}

	@Test
	void workerScheduleIncrementsDirectCounterOnly() {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.immediate(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		test.schedule(() -> {});

		assertThat(testScheduler.submittedDirect.count()).as("submittedDirect.count").isOne();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(testScheduler.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isZero();
		assertThat(testScheduler.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isZero();
	}

	@Test
	void workerScheduleDelayIncrementsDelayedCounter() throws InterruptedException {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		test.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);

		assertThat(testScheduler.submittedDirect.count()).as("submittedDirect.count").isZero();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isOne();
		assertThat(testScheduler.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isZero();
		assertThat(testScheduler.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isZero();
	}

	@Test
	void workerSchedulePeriodicallyIsCorrectlyMetered() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(5);
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		Scheduler.Worker test = testScheduler.createWorker();

		Schedulers.onScheduleHook("test", task -> () -> {
			try {
				task.run();
			}
			finally {
				latch.countDown();
			}
		});

		Disposable d = test.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		latch.await(10, TimeUnit.SECONDS);
		d.dispose();

		assertThat(testScheduler.submittedDirect.count()).as("submittedDirect.count").isZero();
		assertThat(testScheduler.submittedDelayed.count()).as("submittedDelayed.count").isZero();
		assertThat(testScheduler.submittedPeriodicInitial.count()).as("submittedPeriodicInitial.count").isOne();
		assertThat(testScheduler.submittedPeriodicIteration.count()).as("submittedPeriodicIteration.count").isEqualTo(4);
		assertThat(testScheduler.completedTasks.count())
			.as("completed counter tracks all iterations")
			.isEqualTo(5)
			.matches(l -> l == testScheduler.submittedDirect.count()
				+ testScheduler.submittedDelayed.count()
				+ testScheduler.submittedPeriodicInitial.count()
				+ testScheduler.submittedPeriodicIteration.count(), "completed tasks == sum of all timer counts");

		test.dispose();
		Schedulers.resetOnScheduleHook("test");
		testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
	}

	@Test
	void pendingTaskRemovedOnScheduleRejection() throws InterruptedException {
		CountDownLatch activeTaskLatch = new CountDownLatch(1);
		CountDownLatch pendingTaskLatch = new CountDownLatch(1);
		CountDownLatch countPendingLatch = new CountDownLatch(1);
		ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(1));
		Scheduler original = Schedulers.fromExecutorService(executorService);
		TimedScheduler testScheduler = new TimedScheduler(original, registry, "test", Tags.empty());
		testScheduler.init();

		RequiredSearch requiredSearch = registry.get("test.scheduler.tasks.pending");
		LongTaskTimer longTaskTimer = requiredSearch.longTaskTimer();

		try {
			Runnable activeTask = () -> {
				try {
					countPendingLatch.countDown();
					activeTaskLatch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			};

			Runnable pendingTask = pendingTaskLatch::countDown;
			Runnable rejectedTask = () -> {};

			// Schedule two tasks: one will execute, the other will wait in the queue
			assertThatNoException().isThrownBy(() -> testScheduler.schedule(activeTask));
			assertThatNoException().isThrownBy(() -> testScheduler.schedule(pendingTask));

			// Wait till first one is picked up -> exactly one is pending now
			countPendingLatch.await(1, TimeUnit.SECONDS);
			assertThat(longTaskTimer.activeTasks()).as("active pending")
			                                       .isOne();

			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(
					() -> testScheduler.schedule(rejectedTask));
			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(
					() -> testScheduler.schedule(rejectedTask, 0, TimeUnit.SECONDS));

			activeTaskLatch.countDown();
			pendingTaskLatch.await(1, TimeUnit.SECONDS);

			assertThat(longTaskTimer.activeTasks()).as("active pending")
			                                       .isZero();
		} finally {
			testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
		}
	}

	@Test
	void workerPendingTaskRemovedOnScheduleRejection() throws InterruptedException {
		CountDownLatch activeTaskLatch = new CountDownLatch(1);
		CountDownLatch pendingTaskLatch = new CountDownLatch(1);
		CountDownLatch countPendingLatch = new CountDownLatch(1);
		ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(1));
		Scheduler original = Schedulers.fromExecutorService(executorService);
		TimedScheduler testScheduler = new TimedScheduler(original, registry, "test", Tags.empty());
		testScheduler.init();
		Scheduler.Worker worker = testScheduler.createWorker();

		RequiredSearch requiredSearch = registry.get("test.scheduler.tasks.pending");
		LongTaskTimer longTaskTimer = requiredSearch.longTaskTimer();

		try {
			Runnable activeTask = () -> {
				try {
					countPendingLatch.countDown();
					activeTaskLatch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			};

			Runnable pendingTask = pendingTaskLatch::countDown;
			Runnable rejectedTask = () -> {
			};

			// Schedule two tasks: one will execute, the other will wait in the queue
			assertThatNoException().isThrownBy(() -> worker.schedule(activeTask));
			assertThatNoException().isThrownBy(() -> worker.schedule(pendingTask));

			// Wait till first one is picked up -> exactly one is pending now
			countPendingLatch.await(1, TimeUnit.SECONDS);
			assertThat(longTaskTimer.activeTasks()).as("active pending")
			                                       .isOne();

			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(
					() -> worker.schedule(rejectedTask));
			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(
					() -> worker.schedule(rejectedTask, 0, TimeUnit.SECONDS));

			activeTaskLatch.countDown();
			pendingTaskLatch.await(1, TimeUnit.SECONDS);

			assertThat(longTaskTimer.activeTasks()).as("active pending")
			                                       .isZero();
		}
		finally {
			worker.dispose();
			testScheduler.disposeGracefully()
			             .block(Duration.ofSeconds(1));
		}
	}

	@Test
	void pendingTaskRemovedOnCancellation() throws InterruptedException {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		testScheduler.init();

		RequiredSearch tasksPendingSearch = registry.get("test.scheduler.tasks.pending");
		RequiredSearch tasksActiveSearch = registry.get("test.scheduler.tasks.active");
		LongTaskTimer tasksPendingLongTaskTimer = tasksPendingSearch.longTaskTimer();
		LongTaskTimer tasksActiveLongTaskTimer = tasksActiveSearch.longTaskTimer();

		try {
			// Schedule a running task and a pending task
			final CountDownLatch taskPause = new CountDownLatch(1);
			final CountDownLatch taskStarted = new CountDownLatch(1);
			assertThatNoException().isThrownBy(() -> testScheduler.schedule(() -> {
				try {
					taskStarted.countDown();
					taskPause.await();
				}
				catch (InterruptedException e) {
					// Expected as Scheduler is disposed at the end and the task is interrupted
				}
			}));
			Disposable.Swap waitingTask = Disposables.swap();
			assertThatNoException().isThrownBy(() -> waitingTask.update(testScheduler.schedule(() -> {})));

			// We need to wait for the task to start to properly account active vs pending
			assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();

			// One task is pending to be scheduled and one is active
			assertThat(tasksPendingLongTaskTimer.activeTasks()).as("active pending")
					.isEqualTo(1);
			assertThat(tasksActiveLongTaskTimer.activeTasks()).as("active")
					.isEqualTo(1);

			// E.g. a `Mono#timeout` was never hit, so the task gets disposed.
			waitingTask.dispose();

			// The task should no longer be considered pending, as it was disposed.
			assertThat(tasksPendingLongTaskTimer.activeTasks()).as("active pending")
					.isZero();
		} finally {
			testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
		}
	}

	@Test
	void pendingTaskDelayedRemovedOnCancellation() {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		testScheduler.init();

		RequiredSearch requiredSearch = registry.get("test.scheduler.tasks.pending");
		LongTaskTimer longTaskTimer = requiredSearch.longTaskTimer();

		try {
			// Schedule a task far in the future.
			Disposable.Swap waitingTask = Disposables.swap();
			assertThatNoException().isThrownBy(() -> waitingTask.update(testScheduler.schedule(() -> {}, 10_000, TimeUnit.SECONDS)));

			// It's pending to be scheduled.
			assertThat(longTaskTimer.activeTasks()).as("active pending")
					.isOne();

			// E.g. a `Mono#timeout` was never hit, so the task gets disposed.
			waitingTask.dispose();

			// The task should no longer be considered pending, as it was disposed.
			assertThat(longTaskTimer.activeTasks()).as("active pending")
					.isZero();
		} finally {
			testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
		}
	}

	@Test
	void workerPendingTaskRemovedOnCancellation() throws InterruptedException {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		testScheduler.init();
		Scheduler.Worker worker = testScheduler.createWorker();

		RequiredSearch tasksPendingSearch = registry.get("test.scheduler.tasks.pending");
		RequiredSearch tasksActiveSearch = registry.get("test.scheduler.tasks.active");
		LongTaskTimer tasksPendingLongTaskTimer = tasksPendingSearch.longTaskTimer();
		LongTaskTimer tasksActiveLongTaskTimer = tasksActiveSearch.longTaskTimer();

		try {
			// Schedule a running task and a pending task
			final CountDownLatch taskPause = new CountDownLatch(1);
			final CountDownLatch taskStarted = new CountDownLatch(1);
			assertThatNoException().isThrownBy(() -> worker.schedule(() -> {
				try {
					taskStarted.countDown();
					taskPause.await();
				}
				catch (InterruptedException e) {
					// Expected as Scheduler is disposed at the end and the task is interrupted
				}
			}));
			Disposable.Swap waitingTask = Disposables.swap();
			assertThatNoException().isThrownBy(() -> waitingTask.update(worker.schedule(() -> {})));

			// We need to wait for the task to start to properly account active vs pending
			assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();

			// One task is pending to be scheduled and one is active
			assertThat(tasksPendingLongTaskTimer.activeTasks()).as("active pending")
					.isEqualTo(1);
			assertThat(tasksActiveLongTaskTimer.activeTasks()).as("active")
					.isEqualTo(1);

			// E.g. a `Mono#timeout` was never hit, so the task gets disposed.
			waitingTask.dispose();

			// The task should no longer be considered pending, as it was disposed.
			assertThat(tasksPendingLongTaskTimer.activeTasks()).as("active pending")
					.isZero();
		} finally {
			worker.dispose();
			testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
		}
	}

	@Test
	void workerPendingTaskDelayedRemovedOnCancellation() {
		TimedScheduler testScheduler = new TimedScheduler(Schedulers.single(), registry, "test", Tags.empty());
		testScheduler.init();
		Scheduler.Worker worker = testScheduler.createWorker();

		RequiredSearch requiredSearch = registry.get("test.scheduler.tasks.pending");
		LongTaskTimer longTaskTimer = requiredSearch.longTaskTimer();

		try {
			// Schedule a task far in the future.
			Disposable.Swap waitingTask = Disposables.swap();
			assertThatNoException().isThrownBy(() -> waitingTask.update(worker.schedule(() -> {}, 10_000, TimeUnit.SECONDS)));

			// It's pending to be scheduled.
			assertThat(longTaskTimer.activeTasks()).as("active pending")
					.isOne();

			// E.g. a `Mono#timeout` was never hit, so the task gets disposed.
			waitingTask.dispose();

			// The task should no longer be considered pending, as it was disposed.
			assertThat(longTaskTimer.activeTasks()).as("active pending")
					.isZero();
		} finally {
			worker.dispose();
			testScheduler.disposeGracefully().block(Duration.ofSeconds(1));
		}
	}
}
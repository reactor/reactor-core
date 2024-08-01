/*
 * Copyright (c) 2023-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;
import reactor.util.concurrent.Queues;

class BoundedElasticThreadPerTaskSchedulerTest {

	BoundedElasticThreadPerTaskScheduler scheduler;
	List<Disposable>                     disposables = new ArrayList<>();

	@BeforeEach
	void setup() {
		scheduler = newScheduler(2, 3);
		scheduler.init();
	}

	@AfterEach
	void teardown() {
		disposables.forEach(Disposable::dispose);
		disposables.clear();
	}

	BoundedElasticThreadPerTaskScheduler newScheduler(int maxThreads, int maxCapacity) {
		BoundedElasticThreadPerTaskScheduler scheduler =
				new BoundedElasticThreadPerTaskScheduler(maxThreads,
						maxCapacity,
						Thread.ofVirtual()
						      .name("virtualThreadPerTaskBoundedElasticScheduler", 1)
						      .uncaughtExceptionHandler(Schedulers::defaultUncaughtException)
						      .factory());

		disposables.add(scheduler);
		return scheduler;
	}

	@Test
	public void ensuresTasksScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		Disposable disposable = scheduler.schedule(latch::countDown);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresTasksDelayedScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch awaiter = new CountDownLatch(1);

		BoundedElasticThreadPerTaskScheduler.BoundedServices resource =
				scheduler.state.currentResource;
		// submit task which occupy shared single threaded scheduler
		resource.sharedDelayedTasksScheduler.submit(() -> {
			awaiter.await();
			return null;
		});

		// ensures task is picked
		Awaitility.await()
		          .atMost(Duration.ofSeconds(2))
		          .until(() -> ((ScheduledThreadPoolExecutor) resource.sharedDelayedTasksScheduler).getQueue().isEmpty());

		// schedule delayed task which should go to sharedDelayedTasksScheduler
		Disposable disposable = scheduler.schedule(latch::countDown, 1, TimeUnit.MILLISECONDS);

		Assertions.assertThat(((ScheduledThreadPoolExecutor) resource.sharedDelayedTasksScheduler).getQueue().size()).isOne();

		awaiter.countDown();

		Awaitility.await()
				.atMost(Duration.ofSeconds(2))
				.until(() -> ((ScheduledThreadPoolExecutor) resource.sharedDelayedTasksScheduler).getQueue().isEmpty());

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresTasksDelayedZeroDelayScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch awaiter = new CountDownLatch(1);

		BoundedElasticThreadPerTaskScheduler.BoundedServices resource =
				scheduler.state.currentResource;
		// submit task which occupy shared single threaded scheduler
		resource.sharedDelayedTasksScheduler.submit(() -> {
			awaiter.await();
			return null;
		});

		// ensures task is picked
		Awaitility.await()
		          .atMost(Duration.ofSeconds(2))
		          .until(() -> ((ScheduledThreadPoolExecutor) resource.sharedDelayedTasksScheduler).getQueue().isEmpty());

		Disposable disposable = scheduler.schedule(latch::countDown, 0, TimeUnit.MILLISECONDS);

		// assures that no tasks is scheduled for shared scheduler
		Assertions.assertThat(((ScheduledThreadPoolExecutor) resource.sharedDelayedTasksScheduler).getQueue().size()).isZero();

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());

		// unblock scheduler
		awaiter.countDown();
	}

	@Test
	public void ensuresTasksPeriodicScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				1,
				10,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresTasksPeriodicZeroInitialDelayScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				0,
				10,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresTasksPeriodicWithInitialDelayAndInstantPeriodScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				1,
				0,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresTasksPeriodicWithZeroInitialDelayAndInstantPeriodScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				0,
				0,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(disposable.isDisposed()).isTrue());
	}

	@Test
	public void ensuresConcurrentTasksSchedulingWithinSingleWorker() throws InterruptedException {
		Queue<Object> queue = Queues.unboundedMultiproducer()
		                            .get();
		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(2);

			Scheduler.Worker worker = scheduler.createWorker();

			RaceTestUtils.race(() -> worker.schedule(() -> {
				queue.offer("1");
				queue.offer("1");
				queue.offer("1");
				latch.countDown();
			}), () -> worker.schedule(() -> {
				queue.offer("2");
				queue.offer("2");
				queue.offer("2");
				latch.countDown();
			}));

			Assertions.assertThat(latch.await(5, TimeUnit.SECONDS))
			          .isTrue();

			Object value1 = queue.poll();
			Assertions.assertThat(value1).isEqualTo(queue.poll());
			Assertions.assertThat(value1).isEqualTo(queue.poll());

			Object value2 = queue.poll();
			Assertions.assertThat(value2).isEqualTo(queue.poll());
			Assertions.assertThat(value2).isEqualTo(queue.poll());
			worker.dispose();
		}
	}

	@Test
	public void ensuresConcurrentDelayedTasksSchedulingSingleWorker() throws InterruptedException {
		Queue<Object> queue = Queues.unboundedMultiproducer()
		                              .get();
		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(3);

			Scheduler.Worker worker = scheduler.createWorker();

			RaceTestUtils.race(() -> worker.schedule(() -> {
				queue.offer("1");
				queue.offer("1");
				queue.offer("1");
				latch.countDown();
			}, 1, TimeUnit.MILLISECONDS), () -> worker.schedule(() -> {
				queue.offer("2");
				queue.offer("2");
				queue.offer("2");
				latch.countDown();
			}), () -> worker.schedule(() -> {
				queue.offer("3");
				queue.offer("3");
				queue.offer("3");
				latch.countDown();
			}, 1, TimeUnit.MILLISECONDS));

			Assertions.assertThat(latch.await(5, TimeUnit.SECONDS))
			          .isTrue();

			Object value1 = queue.poll();
			Assertions.assertThat(value1).isEqualTo(queue.poll());
			Assertions.assertThat(value1).isEqualTo(queue.poll());

			Object value2 = queue.poll();
			Assertions.assertThat(value2).isEqualTo(queue.poll());
			Assertions.assertThat(value2).isEqualTo(queue.poll());

			Object value3 = queue.poll();
			Assertions.assertThat(value3).isEqualTo(queue.poll());
			Assertions.assertThat(value3).isEqualTo(queue.poll());
			worker.dispose();
		}
	}

	@Test
	public void ensuresConcurrentPeriodicTasksSchedulingSingleWorker() throws InterruptedException {
		Queue<Object> queue = Queues.unboundedMultiproducer()
		                            .get();
		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(10);

			Scheduler.Worker worker = scheduler.createWorker();

			RaceTestUtils.race(() -> worker.schedulePeriodically(() -> {
				queue.offer("1");
				queue.offer("1");
				queue.offer("1");
				latch.countDown();
			}, 1, 0, TimeUnit.MILLISECONDS), () -> worker.schedule(() -> {
				queue.offer("2");
				queue.offer("2");
				queue.offer("2");
				latch.countDown();
			}, 1, TimeUnit.MILLISECONDS), () -> worker.schedulePeriodically(() -> {
				queue.offer("3");
				queue.offer("3");
				queue.offer("3");
				latch.countDown();
			}, 1, 1, TimeUnit.MILLISECONDS));

			Assertions.assertThat(latch.await(5, TimeUnit.SECONDS))
			          .isTrue();

			for (int j = 0; j < 10; j++) {
				Object value = queue.poll();
				Assertions.assertThat(value)
				          .isEqualTo(queue.poll());
				Assertions.assertThat(value)
				          .isEqualTo(queue.poll());
			}
			worker.dispose();
		}
	}

	@Test
	public void ensuresConcurrentWorkerTaskDisposure() throws InterruptedException {
		for (int i = 0; i < 100; i++) {
			CountDownLatch latchNeverReleased = new CountDownLatch(1);
			CountDownLatch firstTaskLatch = new CountDownLatch(1);
			CountDownLatch secondTaskLatch = new CountDownLatch(1);

			Scheduler.Worker worker = scheduler.createWorker();
			Disposable firstTask = worker.schedule(() -> {
				try {
					firstTaskLatch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});
			Disposable secondTask = worker.schedule(() -> {
				try {
					secondTaskLatch.await();
					// The below release is never to be reached.
					// However, we need the above latch to protect against a situation
					// in which during the race:
					// 1. worker is disposed
					// 2. firstTask is cancelled, gets interrupted
					// 3. secondTask is pulled by the worker and executed, so
					//    latchNeverReleased.countDown() is executed
					// 4. secondTask.dispose() is called after the task has already run
					latchNeverReleased.countDown();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});
			RaceTestUtils.race(worker::dispose, secondTask::dispose);

			firstTaskLatch.countDown();
			secondTaskLatch.countDown();

			Assertions.assertThat(latchNeverReleased.getCount()).isOne();
			Assertions.assertThat(worker.isDisposed()).isTrue();
			Assertions.assertThat(firstTask.isDisposed()).isTrue();
			Assertions.assertThat(secondTask.isDisposed()).isTrue();
		}
	}

	@Test
	public void ensuresTasksAreDisposedAndQueueCounterIsDecrementedWhenWorkerIsDisposed() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler
				scheduler = newScheduler(2, 1000);
			scheduler.init();
			Runnable task = () -> {
			};

			for (int i = 0; i < 100; i++) {
				CountDownLatch latch = new CountDownLatch(1);

				Scheduler.Worker worker = scheduler.createWorker();
				List<Disposable> tasks = new ArrayList<>();
				tasks.add(worker.schedule(() -> {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}));

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(2000);
				for (int j = 0; j < 1000; j++) {
					tasks.add(worker.schedule(task));
				}
				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(1000);

				latch.countDown();
				Thread.yield();
				worker.dispose();

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(2000);
				Assertions.assertThat(worker.isDisposed())
				          .isTrue();
				Assertions.assertThat(tasks)
				          .allMatch(Disposable::isDisposed);
			}

	}

	@Test
	public void ensuresTasksAreDisposedAndQueueCounterIsDecrementedWhenAllTasksAreDisposed() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler
				scheduler = newScheduler(2, 1000);

			scheduler.init();
			Runnable task = () -> {
			};

			for (int i = 0; i < 100; i++) {
				CountDownLatch latch = new CountDownLatch(1);

				BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker = (BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();
				List<Disposable> tasks = new ArrayList<>();
				tasks.add(worker.schedule(() -> {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}));

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(2000);
				for (int j = 0; j < 1000; j++) {
					tasks.add(worker.schedule(task));
				}
				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(1000);

				latch.countDown();
				Thread.yield();
				tasks.forEach(Disposable::dispose);

				Awaitility.await()
				          .atMost(Duration.ofSeconds(5))
				          .until(() -> !BoundedElasticThreadPerTaskScheduler.SequentialThreadPerTaskExecutor.hasWork(worker.executor.wipAndRefCnt));

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(2000);
				Assertions.assertThat(worker.isDisposed())
				          .isFalse();
				Assertions.assertThat(tasks)
				          .allMatch(Disposable::isDisposed);
			}
	}

	@Test
	public void ensuresTasksAreDisposedAndQueueCounterIsDecrementedWhenAllTasksAreDisposedDelayedCase() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler
				scheduler = newScheduler(2, 1000);

		scheduler.init();
		Runnable task = () -> {
		};

		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(1);

			BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker = (BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();
			List<Disposable> tasks = new ArrayList<>();
			tasks.add(worker.schedule(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
			          .isEqualTo(2000);
			for (int j = 0; j < 1000; j++) {
				tasks.add(worker.schedulePeriodically(task, 1,1, TimeUnit.MILLISECONDS));
			}
			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
			          .isEqualTo(1000);

			latch.countDown();
			Thread.yield();
			tasks.forEach(Disposable::dispose);

			Awaitility.await()
			          .atMost(Duration.ofSeconds(5))
			          .until(() -> !BoundedElasticThreadPerTaskScheduler.SequentialThreadPerTaskExecutor.hasWork(worker.executor.wipAndRefCnt));

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
			          .isEqualTo(2000);
			Assertions.assertThat(worker.isDisposed())
			          .isFalse();
			Assertions.assertThat(tasks)
			          .allMatch(Disposable::isDisposed);
		}
	}

	@Test
	public void ensuresRandomTasksAreDisposedAndQueueCounterIsDecrementedWhenAllTasksAreDisposed() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler
				scheduler = newScheduler(2, 10000);
			scheduler.init();
			Runnable task = () -> {
			};

			for (int i = 0; i < 100; i++) {
				CountDownLatch latch = new CountDownLatch(1);

				BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker = (BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();
				List<Disposable> tasks = new ArrayList<>();
				tasks.add(worker.schedule(() -> {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}));

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(20000);
				for (int j = 0; j < 10000; j++) {
					switch (ThreadLocalRandom.current()
					                         .nextInt(5)) {
						case 0:
							tasks.add(worker.schedule(task));
							break;
						case 1:
							tasks.add(worker.schedule(task, 1, TimeUnit.NANOSECONDS));
							break;
						case 2:
							tasks.add(worker.schedulePeriodically(task, 1, 1, TimeUnit.NANOSECONDS));
							break;
						case 3:
							tasks.add(worker.schedulePeriodically(task, 0, 1, TimeUnit.NANOSECONDS));
							break;
						case 4:
							tasks.add(worker.schedulePeriodically(task, 0, 0, TimeUnit.NANOSECONDS));
							break;
					}
				}
				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(10000);

				latch.countDown();
				Thread.yield();
				tasks.forEach(Disposable::dispose);

				Awaitility.await()
				          .atMost(Duration.ofSeconds(5))
				          .until(() -> !BoundedElasticThreadPerTaskScheduler.SequentialThreadPerTaskExecutor.hasWork(worker.executor.wipAndRefCnt));

				Assertions.assertThat(scheduler.estimateRemainingTaskCapacity())
				          .isEqualTo(20000);
				Assertions.assertThat(worker.isDisposed())
				          .isFalse();
				Assertions.assertThat(tasks)
				          .allMatch(Disposable::isDisposed);
			}
	}

	@Test
	public void ensuresTasksAreOrderedWithinAWorker() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler scheduler = newScheduler(1, 1000);
		scheduler.init();

		BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker =
				(BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();

		ConcurrentLinkedQueue<Integer> tasksIds = new ConcurrentLinkedQueue<>();

		for (int i = 0; i < 1000; i++) {
			int taskId = i;
			worker.schedule(() -> tasksIds.offer(taskId));
		}


		worker.executor.shutdown(false);
		Assertions.assertThat(worker.executor.await(10, TimeUnit.SECONDS)).isTrue();

		Assertions.assertThat(tasksIds).containsExactlyElementsOf(Flux.range(0, 1000).collectList().block());
	}

	@Test
	public void ensuresDelayedTasksAreOrderedWithinAWorker() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler scheduler = newScheduler(1, 1000);
		scheduler.init();

		BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker =
				(BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();

		ConcurrentLinkedQueue<Integer> tasksIds = new ConcurrentLinkedQueue<>();

		for (int i = 0; i < 1000; i++) {
			int taskId = i;
			worker.schedule(() -> tasksIds.offer(taskId), 1, TimeUnit.MILLISECONDS);
		}

		worker.executor.shutdown(false);
		Assertions.assertThat(worker.executor.await(10, TimeUnit.SECONDS)).isTrue();

		Assertions.assertThat(tasksIds).containsExactlyElementsOf(Flux.range(0, 1000).collectList().block());
	}

	@Test
	public void ensuresWorkersAreNotIntersecting() throws InterruptedException {
		BoundedElasticThreadPerTaskScheduler scheduler = newScheduler(1, 1000);
		scheduler.init();

		BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker worker =
				(BoundedElasticThreadPerTaskScheduler.SingleThreadExecutorWorker) scheduler.createWorker();

		AtomicInteger counter = new AtomicInteger();

		for (int i = 0; i < 500; i++) {
			switch (i % 4) {
				case 0 : worker.schedule(counter::incrementAndGet); break;
				case 1 : worker.schedule(counter::incrementAndGet, 1, TimeUnit.MILLISECONDS); break;
				case 2 : worker.schedulePeriodically(new Runnable() {
					boolean once = false;
					@Override
					public void run() {
						if (!once) {
							once = true;
							counter.incrementAndGet();
						}
					}
				}, 0, 0, TimeUnit.MILLISECONDS); break;
				case 3 : worker.schedulePeriodically(new Runnable() {
					boolean once = false;
					@Override
					public void run() {
						if (!once) {
							once = true;
							counter.incrementAndGet();
						}
					}
				}, 1, 0, TimeUnit.MILLISECONDS); break;
 			}
		}

		for (int i = 0; i < 500; i++) {
			Scheduler.Worker localWorker = scheduler.createWorker();
			switch (ThreadLocalRandom.current().nextInt(0, 3)) {
				case 0 : localWorker.schedule(() -> {}); break;
				case 1 : localWorker.schedule(() -> {}, 1, TimeUnit.MILLISECONDS); break;
				case 2 : localWorker.schedulePeriodically(() -> {}, 1, 1, TimeUnit.MILLISECONDS); break;
			}
			localWorker.dispose();
		}

		worker.executor.shutdown(false);
		Assertions.assertThat(worker.executor.await(10, TimeUnit.SECONDS)).isTrue();

		Assertions.assertThat(counter).hasValue(500);
	}

	@Test
	public void ensuresSupportGracefulShutdown() {
		BoundedElasticThreadPerTaskScheduler scheduler = newScheduler(100, 100_000);
		scheduler.init();

		AtomicInteger counter = new AtomicInteger();

		for (int i = 0; i < 100_000; i++) {
			switch (i % 4) {
				case 0 : scheduler.schedule(counter::incrementAndGet); break;
				case 1 : scheduler.schedule(counter::incrementAndGet, 1, TimeUnit.MILLISECONDS); break;
				case 2 : scheduler.schedulePeriodically(new Runnable() {
					boolean once = false;
					@Override
					public void run() {
						if (!once) {
							once = true;
							counter.incrementAndGet();
						}
					}
				}, 1, 0, TimeUnit.MILLISECONDS); break;
				case 3 : scheduler.schedulePeriodically(new Runnable() {
					boolean once = false;
					@Override
					public void run() {
						if (!once) {
							once = true;
							counter.incrementAndGet();
						}
					}
				}, 0, 0, TimeUnit.MILLISECONDS); break;
				// we can not test that real scheduledAtFixedRate task is awaited since
				// it is not awaited by ScheduledExecutorService, thus no way to
				// observe it
				/*case 4 : scheduler.schedulePeriodically(new Runnable() {
					boolean once = false;
					@Override
					public void run() {
						if (!once) {
							once = true;
							counter.incrementAndGet();
						}
					}
				}, 1, 1, TimeUnit.MILLISECONDS); break;*/
			}
		}

		StepVerifier.create(scheduler.disposeGracefully())
				.expectSubscription()
				.expectComplete()
				.verify(Duration.ofSeconds(100));

		Assertions.assertThat(scheduler.isDisposed()).isTrue();
		Assertions.assertThat(counter).hasValue(100_000);
	}

	@Test
	void ensuresTotalTasksMathIsDoneCorrectlyInOverflow() {
		BoundedElasticThreadPerTaskScheduler scheduler =
				newScheduler(10,
				Integer.MAX_VALUE - 1);
		scheduler.init();
		CountDownLatch latch = new CountDownLatch(1);

		Runnable task = () -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {

			}
		};

		for (int i = 0; i < 10; i++) {
			Scheduler.Worker worker = scheduler.createWorker();
			for (int j = 0; j < 100; j++) {
				worker.schedule(task);
			}
		}

		Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	void ensuresTotalTasksMathIsDoneCorrectlyInEdgeCase() {
		BoundedElasticThreadPerTaskScheduler scheduler =
				newScheduler(10,
						Integer.MAX_VALUE / 10 + 1);
		scheduler.init();
		CountDownLatch latch = new CountDownLatch(1);

		Runnable task = () -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {

			}
		};

		for (int i = 0; i < 10; i++) {
			Scheduler.Worker worker = scheduler.createWorker();
			for (int j = 0; j < 100; j++) {
				worker.schedule(task);
			}
		}

		// Note +10 means that 10 tasks are in fly blocked, and they are not included
		// in the capacity counting since they don't occupy a queue
		Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(10L * (Integer.MAX_VALUE / 10 + 1) - 1000 + 10);
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void allTasksExecuteInParallelWhenMaxProvided(boolean useWorker) throws Exception {
		int total = 2 * Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;

		BoundedElasticThreadPerTaskScheduler scheduler = newScheduler(total, total);
		scheduler.init();

		CountDownLatch latch = new CountDownLatch(total);

		try {
			for (int i = 0; i < total; i++) {
				Runnable task = () -> {
					try {
						latch.countDown();
						latch.await();
					}
					catch (InterruptedException e) {
						// ignore
					}
				};
				if (useWorker) {
					scheduler.createWorker().schedule(task);
				} else {
					scheduler.schedule(task);
				}
			}

			Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
		} finally {
			scheduler.dispose();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void allTasksExecuteInParallelWhenUsingDefaultMax(boolean useWorker) throws Exception {
		int parallelTasks = Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;
		int total = 2 * parallelTasks;

		Scheduler scheduler = Schedulers.boundedElastic();
		scheduler.init();

		CountDownLatch allScheduled = new CountDownLatch(1);

		CountDownLatch secondBatch = new CountDownLatch(parallelTasks);

		try {
			for (int i = 0; i < total; i++) {
				final int taskIndex = i;
				Runnable task = () -> {
					try {
						System.out.println("Task #" + taskIndex);
						allScheduled.await();
						if (taskIndex >= parallelTasks) {
							secondBatch.countDown();
							secondBatch.await();
						}
					}
					catch (InterruptedException e) {
						// ignore
					}
				};
				if (useWorker) {
					scheduler.createWorker().schedule(task);
				} else {
					scheduler.schedule(task);
				}
			}

			allScheduled.countDown();

			Assertions.assertThat(secondBatch.await(1, TimeUnit.SECONDS)).isTrue();
		} finally {
			scheduler.dispose();
		}
	}
}
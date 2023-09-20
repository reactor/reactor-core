package reactor.core.scheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.util.RaceTestUtils;
import reactor.util.concurrent.Queues;

class LoomBoundedElasticSchedulerTest {

	Scheduler scheduler;

	@BeforeEach
	void setup() {
		scheduler = new LoomBoundedElasticScheduler(2,
				3,
				Thread.ofVirtual()
				      .name("loom", 0)
				      .factory());
		scheduler.init();
	}

	@AfterEach
	void teardown() {
		scheduler.dispose();
	}

	@Test
	public void ensuresTasksScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		Disposable disposable = scheduler.schedule(latch::countDown);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isTrue();
	}

	@Test
	public void ensuresTasksDelayedScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		Disposable disposable = scheduler.schedule(latch::countDown, 200, TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isTrue();
	}

	@Test
	public void ensuresTasksDelayedZeroDelayScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		Disposable disposable = scheduler.schedule(latch::countDown, 0, TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isTrue();
	}

	@Test
	public void ensuresTasksPeriodicScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				100,
				10,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Assertions.assertThat(disposable.isDisposed()).isTrue();
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
		Assertions.assertThat(disposable.isDisposed()).isTrue();
	}

	@Test
	public void ensuresTasksPeriodicWithInitialDelayAndInstantPeriodScheduling() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(10);

		Disposable disposable = scheduler.schedulePeriodically(latch::countDown,
				100,
				0,
				TimeUnit.MILLISECONDS);

		Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(disposable.isDisposed()).isFalse();
		disposable.dispose();
		Assertions.assertThat(disposable.isDisposed()).isTrue();
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
		Assertions.assertThat(disposable.isDisposed()).isTrue();
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
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);

			Scheduler.Worker worker = scheduler.createWorker();
			worker.schedule(()-> {
				try {
					latch2.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});
			Disposable disposable = worker.schedule(latch::countDown);
			RaceTestUtils.race(() -> worker.dispose(), () -> disposable.dispose());
			latch2.countDown();
			Assertions.assertThat(latch.getCount())
			          .isOne();
			Assertions.assertThat(worker.isDisposed()).isTrue();
			Assertions.assertThat(disposable.isDisposed()).isTrue();
		}
	}

	@Test
	public void ensuresTasksAreDisposedAndQueueCounterIsDecrementedWhenWorkerIsDisposed() throws InterruptedException {
		LoomBoundedElasticScheduler scheduler = new LoomBoundedElasticScheduler(2,
				1000,
				Thread.ofVirtual()
				      .name("loom", 0)
				      .factory());
		scheduler.init();
		Runnable task = () -> {};

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

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(2000);
			for (int j = 0; j < 1000; j++) {
				tasks.add(worker.schedule(task));
			}
			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(1000);

			latch.countDown();
			Thread.yield();
			worker.dispose();

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(2000);
			Assertions.assertThat(worker.isDisposed()).isTrue();
			Assertions.assertThat(tasks).allMatch(Disposable::isDisposed);
		}
	}

	@Test
	public void ensuresTasksAreDisposedAndQueueCounterIsDecrementedWhenAllTasksAreDisposed() throws InterruptedException {
		LoomBoundedElasticScheduler scheduler = new LoomBoundedElasticScheduler(2,
				1000,
				Thread.ofVirtual()
				      .name("loom", 0)
					  .uncaughtExceptionHandler((tr, t) -> {
						  System.out.println("Dropping " + t + " from " + tr);
					  })
				      .factory());
		scheduler.init();
		Runnable task = () -> {};

		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(1);

			LoomBoundedElasticScheduler.SingleThreadExecutorWorker worker =
					(LoomBoundedElasticScheduler.SingleThreadExecutorWorker) scheduler.createWorker();
			List<Disposable> tasks = new ArrayList<>();
			tasks.add(worker.schedule(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(2000);
			for (int j = 0; j < 1000; j++) {
				tasks.add(worker.schedule(task));
			}
			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(1000);

			latch.countDown();
			Thread.yield();
			tasks.forEach(Disposable::dispose);

			Awaitility.await()
			          .atMost(Duration.ofSeconds(5))
			          .until(() -> worker.executor.state == 0);

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(2000);
			Assertions.assertThat(worker.isDisposed()).isFalse();
			Assertions.assertThat(tasks).allMatch(Disposable::isDisposed);
		}
	}

	@Test
	public void ensuresRandomTasksAreDisposedAndQueueCounterIsDecrementedWhenAllTasksAreDisposed() throws InterruptedException {
		LoomBoundedElasticScheduler scheduler = new LoomBoundedElasticScheduler(2,
				10000,
				Thread.ofVirtual()
				      .name("loom", 0)
				      .uncaughtExceptionHandler((tr, t) -> {
					      System.out.println("Dropping " + t + " from " + tr);
				      })
				      .factory());
		scheduler.init();
		Runnable task = () -> {};

		for (int i = 0; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(1);

			LoomBoundedElasticScheduler.SingleThreadExecutorWorker worker =
					(LoomBoundedElasticScheduler.SingleThreadExecutorWorker) scheduler.createWorker();
			List<Disposable> tasks = new ArrayList<>();
			tasks.add(worker.schedule(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(20000);
			for (int j = 0; j < 10000; j++) {
				switch (ThreadLocalRandom.current().nextInt(5)) {
					case 0: tasks.add(worker.schedule(task)); break;
					case 1: tasks.add(worker.schedule(task, 1, TimeUnit.NANOSECONDS)); break;
					case 2: tasks.add(worker.schedulePeriodically(task, 1, 1, TimeUnit.NANOSECONDS)); break;
					case 3: tasks.add(worker.schedulePeriodically(task, 0, 1, TimeUnit.NANOSECONDS)); break;
					case 4: tasks.add(worker.schedulePeriodically(task, 0, 0, TimeUnit.NANOSECONDS)); break;
				}
			}
			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(10000);

			latch.countDown();
			Thread.yield();
			tasks.forEach(Disposable::dispose);

			Awaitility.await()
			          .atMost(Duration.ofSeconds(5))
			          .until(() -> worker.executor.state == 0);

			Assertions.assertThat(scheduler.estimateRemainingTaskCapacity()).isEqualTo(20000);
			Assertions.assertThat(worker.isDisposed()).isFalse();
			Assertions.assertThat(tasks).allMatch(Disposable::isDisposed);
		}
	}
}
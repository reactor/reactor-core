/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.pivovarit.function.ThrowingRunnable;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Test;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
public class BoundedElasticSchedulerTest extends AbstractSchedulerTest {

	private static final Logger LOGGER = Loggers.getLogger(BoundedElasticSchedulerTest.class);

	static Stream<String> dumpThreadNames() {
		Thread[] tarray;
		for(;;) {
			tarray = new Thread[Thread.activeCount()];
			int dumped = Thread.enumerate(tarray);
			if (dumped <= tarray.length) {
				break;
			}
		}
		return Arrays.stream(tarray).filter(Objects::nonNull).map(Thread::getName);
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@AfterClass
	public static void dumpThreads() {
		LOGGER.debug("Remaining threads after test class:");
		LOGGER.debug(dumpThreadNames().collect(Collectors.joining(", ")));
	}

	@Override
	protected Scheduler scheduler() {
		return afterTest.autoDispose(
				Schedulers.newBoundedElastic(
						4, Integer.MAX_VALUE,
						new ReactorThreadFactory("boundedElasticSchedulerTest", BoundedElasticScheduler.COUNTER,
								false, false, Schedulers::defaultUncaughtException),
						10
				));
	}

	@Test
	public void extraTasksAreQueuedInVirtualWorker() throws InterruptedException {
		AtomicInteger taskRun = new AtomicInteger();
		Scheduler s = schedulerNotCached();

		Scheduler.Worker worker1 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker2 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker3 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker4 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker5 = afterTest.autoDispose(s.createWorker());

		assertThat(worker1).isExactlyInstanceOf(BoundedElasticScheduler.ActiveWorker.class);
		assertThat(worker2).isExactlyInstanceOf(BoundedElasticScheduler.ActiveWorker.class);
		assertThat(worker3).isExactlyInstanceOf(BoundedElasticScheduler.ActiveWorker.class);
		assertThat(worker4).isExactlyInstanceOf(BoundedElasticScheduler.ActiveWorker.class);
		assertThat(worker5).isExactlyInstanceOf(BoundedElasticScheduler.DeferredWorker.class);

		worker1.schedule(() -> {});
		worker2.schedule(() -> {});
		worker3.schedule(() -> {});
		worker4.schedule(() -> {});
		Disposable periodicDeferredTask = worker5.schedulePeriodically(taskRun::incrementAndGet, 0L, 100, TimeUnit.MILLISECONDS);

		Awaitility.with().pollDelay(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(taskRun).as("task held due to worker cap").hasValue(0));

		worker1.dispose(); //should trigger work stealing of worker5

		Awaitility.waitAtMost(250, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(taskRun).as("task running periodically").hasValue(3));

		periodicDeferredTask.dispose();

		int onceCancelled = taskRun.get();
		Awaitility.with()
		          .pollDelay(200, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(taskRun).as("task has stopped").hasValue(onceCancelled));
	}

	@Test
	public void whenCapReachedDirectTasksAreTurnedIntoDeferredFacades() throws InterruptedException {
		Scheduler s = schedulerNotCached();
		//reach the cap of workers
		Scheduler.Worker worker1 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker2 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker3 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker4 = afterTest.autoDispose(s.createWorker());

		Disposable extraDirect1 = afterTest.autoDispose(s.schedule(() -> { }));
		Disposable extraDirect2 = afterTest.autoDispose(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		Disposable extraDirect3 = afterTest.autoDispose(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));

		assertThat(extraDirect1)
				.as("extraDirect1")
				.isNotSameAs(extraDirect2)
				.isNotSameAs(extraDirect3)
				.isInstanceOf(BoundedElasticScheduler.DeferredDirect.class);

		assertThat(extraDirect2)
				.as("extraDirect2")
				.isNotSameAs(extraDirect1)
				.isNotSameAs(extraDirect3)
				.isInstanceOf(BoundedElasticScheduler.DeferredDirect.class);

		assertThat(extraDirect3)
				.as("extraDirect3")
				.isInstanceOf(BoundedElasticScheduler.DeferredDirect.class);
	}

	// below tests similar to ElasticScheduler
	@Test
	public void unsupportedStart() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(scheduler()::start);
	}

	@Test
	public void negativeTtl() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, Integer.MAX_VALUE,null, -1));
	}

	@Test
	public void negativeThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(-1, Integer.MAX_VALUE, null, 1));
	}

	@Test
	public void zeroThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(0, Integer.MAX_VALUE, null, 1));
	}

	@Test
	public void negativeTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, -1, null, 1));
	}

	@Test
	public void zeroTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, 0, null, 1));
	}

	@Test
	public void evictionForWorkerScheduling() {
		BoundedElasticScheduler
				s = afterTest.autoDispose(new BoundedElasticScheduler(2, Integer.MAX_VALUE, r -> new Thread(r, "eviction"), 1));

		Scheduler.Worker worker1 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker2 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker3 = afterTest.autoDispose(s.createWorker());

		assertThat(s.allServices).as("3 workers equals 2 executors").hasSize(2);
		assertThat(s.deferredFacades).as("3 workers equals 1 deferred").hasSize(1);
		assertThat(s.idleServicesWithExpiry).as("no worker expiry").isEmpty();

		worker1.dispose();
		assertThat(s.idleServicesWithExpiry).as("deferred worker activated: no expiry").isEmpty();
		assertThat(s.deferredFacades).as("deferred worker activated: no deferred").isEmpty();

		worker2.dispose();
		worker3.dispose();

		Awaitility.with()
		          .pollInterval(50, TimeUnit.MILLISECONDS)
		          .await()
		          //the evictor in the background can and does have a shift, but not more than 1s
		          .between(1, TimeUnit.SECONDS, 2500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> {
		          	assertThat(s.allServices).hasSize(0);
		          	assertThat(s.deferredFacades).hasSize(0);
		          });
	}

	@Test
	public void evictionForDirectScheduling() {
		BoundedElasticScheduler
				s = afterTest.autoDispose(new BoundedElasticScheduler(2, Integer.MAX_VALUE, r -> new Thread(r, "eviction"), 1));

		Scheduler.Worker worker1 = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker2 = afterTest.autoDispose(s.createWorker());

		AtomicReference<String> taskRanIn = new AtomicReference<>();
		afterTest.autoDispose(s.schedule(() -> taskRanIn.set(Thread.currentThread().getName())));

		assertThat(taskRanIn).as("before thread freed")
		                     .hasValue(null);

		worker1.dispose();
		worker2.dispose();

		Awaitility.with()
		          .pollInterval(50, TimeUnit.MILLISECONDS)
		          .await()
		          //the evictor in the background can and does have a shift, but not more than 1s
		          .between(1, TimeUnit.SECONDS, 2500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> {
			          assertThat(s.allServices).as("allServices").hasSize(0);
			          assertThat(s.deferredFacades).as("deferredWorkers").hasSize(0);
			          assertThat(taskRanIn).as("task ran").doesNotHaveValue(null);

			          assertThat(dumpThreadNames())
					          .as("threads")
					          .doesNotContain(taskRanIn.get());
		          });
	}

	@Test
	public void lifoEviction() throws InterruptedException {
		Scheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(200, Integer.MAX_VALUE, r -> new Thread(r, "dequeueEviction"), 1));
		int otherThreads = Thread.activeCount();
		try {

			int cacheSleep = 100; //slow tasks last 100ms
			int cacheCount = 100; //100 of slow tasks
			int fastSleep = 10;   //interval between fastTask scheduling
			int fastCount = 200;  //will schedule fast tasks up to 2s later
			CountDownLatch latch = new CountDownLatch(cacheCount + fastCount);
			for (int i = 0; i < cacheCount; i++) {
				Mono.fromRunnable(ThrowingRunnable.unchecked(() -> Thread.sleep(cacheSleep)))
				    .subscribeOn(scheduler)
				    .doFinally(sig -> latch.countDown())
				    .subscribe();
			}

			int oldActive = 0;
			int activeAtBeginning = 0;
			int activeAtEnd = Integer.MAX_VALUE;
			for (int i = 0; i < fastCount; i++) {
				Mono.just(i)
				    .subscribeOn(scheduler)
				    .doFinally(sig -> latch.countDown())
				    .subscribe();

				if (i == 0) {
					activeAtBeginning = Thread.activeCount() - otherThreads;
					oldActive = activeAtBeginning;
					LOGGER.info("{} threads active in round 1/{}", activeAtBeginning, fastCount);
				}
				else if (i == fastCount - 1) {
					activeAtEnd = Thread.activeCount() - otherThreads;
					LOGGER.info("{} threads active in round {}/{}", activeAtEnd, i + 1, fastCount);
				}
				else {
					int newActive = Thread.activeCount() - otherThreads;
					if (oldActive != newActive) {
						oldActive = newActive;
						LOGGER.info("{} threads active in round {}/{}", oldActive, i + 1, fastCount);
					}
				}
				Thread.sleep(fastSleep);
			}

			assertThat(latch.await(3, TimeUnit.SECONDS)).as("latch 3s").isTrue();
			assertThat(activeAtEnd).as("active in last round")
			                       .isLessThan(activeAtBeginning)
			                       .isCloseTo(1, Offset.offset(5));
		}
		finally {
			scheduler.dispose();
			LOGGER.info("{} threads active post shutdown", Thread.activeCount() - otherThreads);
		}
	}

	@Test
	public void userWorkerShutdownBySchedulerDisposal() throws InterruptedException {
		Scheduler s = afterTest.autoDispose(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "boundedElasticUserThread", 10, false));
		Scheduler.Worker w = afterTest.autoDispose(s.createWorker());

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> threadName = new AtomicReference<>();

		w.schedule(() -> {
			threadName.set(Thread.currentThread().getName());
			latch.countDown();
		});

		assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch 5s").isTrue();

		s.dispose();

		Awaitility.with().pollInterval(100, TimeUnit.MILLISECONDS)
		          .await().atMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(dumpThreadNames()).doesNotContain(threadName.get()));
	}

	@Test
	public void deferredWorkerDisposedEarly() {
		BoundedElasticScheduler s = afterTest.autoDispose(new BoundedElasticScheduler(1, Integer.MAX_VALUE, Thread::new,10));
		Scheduler.Worker firstWorker = afterTest.autoDispose(s.createWorker());
		Scheduler.Worker worker = s.createWorker();

		assertThat(s.deferredFacades).as("deferred workers before inverted dispose").hasSize(1);
		assertThat(s.idleServicesWithExpiry).as("threads before inverted dispose").isEmpty();

		worker.dispose();
		firstWorker.dispose();

		assertThat(s.deferredFacades).as("deferred workers after inverted dispose").isEmpty();
		assertThat(s.idleServicesWithExpiry).as("threads after inverted dispose").hasSize(1);
	}

	@Test
	public void regrowFromEviction() throws InterruptedException {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "regrowFromEviction", 1));
		Scheduler.Worker worker = scheduler.createWorker();
		worker.schedule(() -> {});

		List<BoundedElasticScheduler.CachedService> beforeEviction = new ArrayList<>(scheduler.allServices);
		assertThat(beforeEviction)
				.as("before eviction")
				.hasSize(1);

		worker.dispose();
		//simulate an eviction 1s in the future
		long fakeNow = System.currentTimeMillis() + 1001;
		scheduler.eviction(() -> fakeNow);

		assertThat(scheduler.allServices)
				.as("after eviction")
				.isEmpty();

		Scheduler.Worker regrowWorker = afterTest.autoDispose(scheduler.createWorker());
		assertThat(regrowWorker).isInstanceOf(BoundedElasticScheduler.ActiveWorker.class);
		regrowWorker.schedule(() -> {});

		assertThat(scheduler.allServices)
				.as("after regrowth")
				.isNotEmpty()
				.hasSize(1)
				.doesNotContainAnyElementsOf(beforeEviction);
	}

	@Test
	public void taskCapIsSharedBetweenDirectAndIndirectDeferredScheduling() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 9, Thread::new, 10));
		Scheduler.Worker activeWorker = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		Scheduler.Worker deferredWorker1 = boundedElasticScheduler.createWorker();
		Scheduler.Worker deferredWorker2 = boundedElasticScheduler.createWorker();

		//enqueue tasks in first deferred worker
		deferredWorker1.schedule(() -> {});
		deferredWorker1.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		deferredWorker1.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks in second deferred worker
		deferredWorker2.schedule(() -> {});
		deferredWorker2.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		deferredWorker2.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks directly on scheduler
		boundedElasticScheduler.schedule(() -> {});
		boundedElasticScheduler.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		boundedElasticScheduler.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//any attempt at scheduling more task should result in rejection
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(0);
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 immediate").isThrownBy(() -> deferredWorker1.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 delayed").isThrownBy(() -> deferredWorker1.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 periodic").isThrownBy(() -> deferredWorker1.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 immediate").isThrownBy(() -> deferredWorker2.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 delayed").isThrownBy(() -> deferredWorker2.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 periodic").isThrownBy(() -> deferredWorker2.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler immediate").isThrownBy(() -> boundedElasticScheduler.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler delayed").isThrownBy(() -> boundedElasticScheduler.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler periodic").isThrownBy(() -> boundedElasticScheduler.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void taskCapResetWhenDirectDeferredTaskIsExecuted() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = boundedElasticScheduler.createWorker();

		//enqueue tasks directly on scheduler
		boundedElasticScheduler.schedule(() -> {});
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(0);

		activeWorker.dispose();
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenWorkerDeferredTaskIsExecuted() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = boundedElasticScheduler.createWorker();
		Scheduler.Worker deferredWorker = boundedElasticScheduler.createWorker();

		//enqueue tasks on deferred worker
		deferredWorker.schedule(() -> {});
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(0);

		activeWorker.dispose();
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenDirectDeferredTaskIsDisposed() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = boundedElasticScheduler.createWorker();

		Disposable d = boundedElasticScheduler.schedule(() -> {});
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(0);

		d.dispose();
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenWorkerDeferredTaskIsDisposed() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = boundedElasticScheduler.createWorker();
		Scheduler.Worker deferredWorker = boundedElasticScheduler.createWorker();

		//enqueue tasks on deferred worker
		Disposable d = deferredWorker.schedule(() -> {});
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(0);

		d.dispose();
		assertThat(boundedElasticScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void deferredWorkerTasksEventuallyExecuted() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker deferredWorker = afterTest.autoDispose(scheduler.createWorker());

		AtomicInteger runCount = new AtomicInteger();
		deferredWorker.schedule(runCount::incrementAndGet);
		deferredWorker.schedule(runCount::incrementAndGet, 10, TimeUnit.MILLISECONDS);
		deferredWorker.schedulePeriodically(runCount::incrementAndGet, 10, 100_000, TimeUnit.MILLISECONDS);

		assertThat(runCount).hasValue(0);
		activeWorker.dispose();

		Awaitility.with().pollDelay(0, TimeUnit.MILLISECONDS).and().pollInterval(10, TimeUnit.MILLISECONDS)
		          .await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(runCount).hasValue(3));
	}

	@Test
	public void deferredDirectTasksEventuallyExecuted() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());

		AtomicInteger runCount = new AtomicInteger();
		scheduler.schedule(runCount::incrementAndGet);
		scheduler.schedule(runCount::incrementAndGet, 10, TimeUnit.MILLISECONDS);
		scheduler.schedulePeriodically(runCount::incrementAndGet, 10, 100_000, TimeUnit.MILLISECONDS);

		assertThat(runCount).hasValue(0);
		activeWorker.dispose();

		Awaitility.with().pollDelay(0, TimeUnit.MILLISECONDS).and().pollInterval(10, TimeUnit.MILLISECONDS)
		          .await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(runCount).hasValue(3));
	}

	@Test
	public void deferredWorkerDisposalRemovesFromFacadeQueue() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker deferredWorker = afterTest.autoDispose(scheduler.createWorker());

		assertThat(scheduler.deferredFacades).as("before dispose")
		                                     .hasSize(1)
		                                     .containsExactly((BoundedElasticScheduler.DeferredFacade) deferredWorker);

		deferredWorker.dispose();
		assertThat(scheduler.deferredFacades).as("after dispose").isEmpty();
	}

	@Test
	public void deferredDirectDisposalRemovesFromFacadeQueue() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Disposable deferredDirect = scheduler.schedule(() -> {});

		assertThat(scheduler.deferredFacades).as("before dispose")
		                                     .hasSize(1)
		                                     .containsExactly((BoundedElasticScheduler.DeferredFacade) deferredDirect);

		deferredDirect.dispose();
		assertThat(scheduler.deferredFacades).as("after dispose").isEmpty();
	}

	@Test
	public void deferredWorkerSetServiceIgnoredIfDisposed() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		BoundedElasticScheduler.ActiveWorker activeWorker = (BoundedElasticScheduler.ActiveWorker) afterTest.autoDispose(scheduler.createWorker());
		BoundedElasticScheduler.DeferredWorker deferredWorker = (BoundedElasticScheduler.DeferredWorker) afterTest.autoDispose(scheduler.createWorker());

		deferredWorker.dispose();

		assertThat(scheduler.idleServicesWithExpiry).isEmpty();
		deferredWorker.setService(afterTest.autoDispose(new BoundedElasticScheduler.CachedService(scheduler)));

		assertThat(scheduler.idleServicesWithExpiry).hasSize(1);
	}

	@Test
	public void deferredDirectSetServiceIgnoredIfDisposed() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		BoundedElasticScheduler.ActiveWorker activeWorker = (BoundedElasticScheduler.ActiveWorker) afterTest.autoDispose(scheduler.createWorker());
		BoundedElasticScheduler.DeferredDirect deferredDirect = (BoundedElasticScheduler.DeferredDirect) afterTest.autoDispose(scheduler.schedule(() -> {}));

		deferredDirect.dispose();

		assertThat(scheduler.idleServicesWithExpiry).isEmpty();
		deferredDirect.setService(afterTest.autoDispose(new BoundedElasticScheduler.CachedService(scheduler)));

		assertThat(scheduler.idleServicesWithExpiry).hasSize(1);
	}

	@Test
	public void deferredWorkerRejectsTasksAfterBeingDisposed() {
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		BoundedElasticScheduler.DeferredWorker deferredWorker = new BoundedElasticScheduler.DeferredWorker(scheduler);
		deferredWorker.dispose();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> deferredWorker.schedule(() -> {}))
				.as("immediate schedule after dispose")
				.withMessage("Worker has been disposed");

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> deferredWorker.schedule(() -> {}, 10, TimeUnit.MILLISECONDS))
				.as("delayed schedule after dispose")
				.withMessage("Worker has been disposed");

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> deferredWorker.schedulePeriodically(() -> {}, 10, 10, TimeUnit.MILLISECONDS))
				.as("periodic schedule after dispose")
				.withMessage("Worker has been disposed");
	}


	@Test
	public void mixOfDirectAndWorkerTasksWithRejectionAfter100kLimit() {
		AtomicInteger taskDone = new AtomicInteger();
		AtomicInteger taskRejected = new AtomicInteger();

		int limit = 100_000;
		int workerCount = 70_000;

		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(
				1, limit,
				"tasksRejectionAfter100kLimit"
		));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker fakeWorker = afterTest.autoDispose(scheduler.createWorker());

		for (int i = 0; i < limit + 10; i++) {
			try {
				if (i < workerCount) {
					//larger subset of tasks are submitted to the worker
					fakeWorker.schedule(taskDone::incrementAndGet);
				}
				else if (i < limit) {
					//smaller subset of tasks are submitted directly to the scheduler
					scheduler.schedule(taskDone::incrementAndGet);
				}
				else if (i % 2 == 0) {
					//half of over-limit tasks are submitted directly to the scheduler, half to worker
					scheduler.schedule(taskDone::incrementAndGet);
				}
				else {
					//half of over-limit tasks are submitted directly to the scheduler, half to worker
					fakeWorker.schedule(taskDone::incrementAndGet);
				}
			}
			catch (RejectedExecutionException ree) {
				taskRejected.incrementAndGet();
			}
		}

		assertThat(taskDone).as("taskDone before releasing activeWorker").hasValue(0);
		assertThat(taskRejected).as("task rejected").hasValue(10);

		activeWorker.dispose();

		Awaitility.await().atMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() ->
				          assertThat(taskDone).as("all fakeWorker tasks done")
				                              .hasValue(workerCount)
		          );

		fakeWorker.dispose();

		//TODO maybe investigate: large amount of direct deferred tasks takes more time to be executed
		Awaitility.await().atMost(10, TimeUnit.SECONDS)
		          .untilAsserted(() ->
				          assertThat(taskDone).as("all deferred tasks done")
				                              .hasValue(limit)
		          );
	}

	@Test
	public void defaultBoundedElasticConfigurationIsConsistentWithJavadoc() {
		Schedulers.CachedScheduler cachedBoundedElastic = (Schedulers.CachedScheduler) Schedulers.boundedElastic();
		BoundedElasticScheduler boundedElastic = (BoundedElasticScheduler) cachedBoundedElastic.cached;

		//10 x number of CPUs
		assertThat(boundedElastic.threadCap)
				.as("default boundedElastic size")
				.isEqualTo(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE)
				.isEqualTo(Runtime.getRuntime().availableProcessors() * 10);

		//60s TTL
		assertThat(boundedElastic.ttlSeconds)
				.as("default TTL")
				.isEqualTo(BoundedElasticScheduler.DEFAULT_TTL_SECONDS)
				.isEqualTo(60);

		//100K bounded task queueing
		assertThat(boundedElastic.deferredTaskCap)
				.as("default unbounded task queueing")
				.isEqualTo(100_000)
				.isEqualTo(Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE);
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "scanName", 3));
		Scheduler withBasicFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, Thread::new, 3));
		Scheduler withTaskCap = afterTest.autoDispose(Schedulers.newBoundedElastic(1, 123, Thread::new, 3));
		Scheduler cached = Schedulers.boundedElastic();

		Scheduler.Worker workerWithNamedFactory = afterTest.autoDispose(withNamedFactory.createWorker());
		Scheduler.Worker deferredWorkerWithNamedFactory = afterTest.autoDispose(withNamedFactory.createWorker());
		Scheduler.Worker workerWithBasicFactory = afterTest.autoDispose(withBasicFactory.createWorker());
		Scheduler.Worker deferredWorkerWithBasicFactory = afterTest.autoDispose(withBasicFactory.createWorker());

		assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
				.as("withNamedFactory")
				.isEqualTo("boundedElastic(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s)");

		assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
				.as("withBasicFactory")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueued=unbounded,ttl=3s)");

		assertThat(Scannable.from(withTaskCap).scan(Scannable.Attr.NAME))
				.as("withTaskCap")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueued=123,ttl=3s)");

		assertThat(cached)
				.as("boundedElastic() is cached")
				.is(SchedulersTest.CACHED_SCHEDULER);
		assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
				.as("default boundedElastic()")
				.isEqualTo("Schedulers.boundedElastic()");

		assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("workerWithNamedFactory")
				.isEqualTo("boundedElastic(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s).worker");

		assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("workerWithBasicFactory")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueued=unbounded,ttl=3s).worker");

		assertThat(Scannable.from(deferredWorkerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithNamedFactory")
				.isEqualTo("boundedElastic(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s).deferredWorker");

		assertThat(Scannable.from(deferredWorkerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithBasicFactory")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueued=unbounded,ttl=3s).deferredWorker");
	}

	@Test
	public void scanCapacity() {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, Thread::new, 2));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker deferredWorker = afterTest.autoDispose(scheduler.createWorker());

		//smoke test that second worker is a DeferredWorker
		assertThat(deferredWorker).as("check second worker is deferred").isExactlyInstanceOf(
				BoundedElasticScheduler.DeferredWorker.class);

		assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler capacity").isEqualTo(1);
		assertThat(Scannable.from(activeWorker).scan(Scannable.Attr.CAPACITY)).as("active worker capacity").isEqualTo(1);
		assertThat(Scannable.from(deferredWorker).scan(Scannable.Attr.CAPACITY)).as("deferred worker capacity").isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void scanDeferredDirect() {
		BoundedElasticScheduler parent = afterTest.autoDispose(new BoundedElasticScheduler(3, 10, Thread::new, 60));
		BoundedElasticScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);

		BoundedElasticScheduler.DeferredDirect deferredDirect = new BoundedElasticScheduler.DeferredDirect(() -> {}, 10, 10, TimeUnit.MILLISECONDS, parent);

		assertThat(deferredDirect.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
		assertThat(deferredDirect.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isFalse();

		assertThat(deferredDirect.scan(Scannable.Attr.NAME)).as("NAME").isEqualTo(parent.toString() + ".deferredDirect");

		assertThat(deferredDirect.scan(Scannable.Attr.CAPACITY)).as("CAPACITY").isOne();
		assertThat(deferredDirect.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);

		//BUFFERED 1 as long as not realized
		assertThat(deferredDirect.scan(Scannable.Attr.BUFFERED)).as("BUFFERED").isOne();
		BoundedElasticScheduler.CachedService cachedService = afterTest.autoDispose(new BoundedElasticScheduler.CachedService(parent));
		deferredDirect.set(cachedService);
		assertThat(deferredDirect.scan(Scannable.Attr.BUFFERED)).as("BUFFERED with CachedService").isZero();

		deferredDirect.dispose();
		assertThat(deferredDirect.scan(Scannable.Attr.TERMINATED)).as("TERMINATED once disposed").isTrue();
		assertThat(deferredDirect.scan(Scannable.Attr.CANCELLED)).as("CANCELLED once disposed").isTrue();
	}

	@Test
	public void scanDeferredWorker() {
		BoundedElasticScheduler parent = afterTest.autoDispose(new BoundedElasticScheduler(3, 10, Thread::new, 60));
		BoundedElasticScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);

		BoundedElasticScheduler.DeferredWorker deferredWorker = new BoundedElasticScheduler.DeferredWorker(parent);

		assertThat(deferredWorker.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
		assertThat(deferredWorker.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isFalse();
		assertThat(deferredWorker.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(deferredWorker.scan(Scannable.Attr.NAME)).as("NAME").isEqualTo(parent.toString() + ".deferredWorker");

		//capacity depends on parent's remaining tasks
		assertThat(deferredWorker.scan(Scannable.Attr.CAPACITY)).as("CAPACITY").isEqualTo(9);
		BoundedElasticScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);
		assertThat(deferredWorker.scan(Scannable.Attr.CAPACITY)).as("CAPACITY after remaingTask decrement").isEqualTo(8);


		//BUFFERED depends on tasks queue size
		assertThat(deferredWorker.scan(Scannable.Attr.BUFFERED)).as("BUFFERED").isZero();
		deferredWorker.schedule(() -> {});
		deferredWorker.schedule(() -> {});
		assertThat(deferredWorker.scan(Scannable.Attr.BUFFERED)).as("BUFFERED once tasks submitted").isEqualTo(2);

		deferredWorker.dispose();
		assertThat(deferredWorker.scan(Scannable.Attr.TERMINATED)).as("TERMINATED once disposed").isTrue();
		assertThat(deferredWorker.scan(Scannable.Attr.CANCELLED)).as("CANCELLED once disposed").isTrue();
	}
}
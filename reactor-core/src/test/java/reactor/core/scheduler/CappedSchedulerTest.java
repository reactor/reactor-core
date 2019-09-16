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

import java.sql.SQLOutput;
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
import org.mockito.Mockito;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
public class CappedSchedulerTest extends AbstractSchedulerTest {

	private static final Logger LOGGER = Loggers.getLogger(CappedSchedulerTest.class);

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
		return autoCleanup(
				Schedulers.newCapped(
						4,
						new ReactorThreadFactory("cappedSchedulerTest", CappedScheduler.COUNTER,
								false, false, Schedulers::defaultUncaughtException),
						10
				));
	}

	@Test
	public void extraTasksAreQueuedInVirtualWorker() throws InterruptedException {
		AtomicInteger taskRun = new AtomicInteger();
		Scheduler s = schedulerNotCached();

		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());
		Scheduler.Worker worker3 = autoCleanup(s.createWorker());
		Scheduler.Worker worker4 = autoCleanup(s.createWorker());
		Scheduler.Worker worker5 = autoCleanup(s.createWorker());

		assertThat(worker1).isExactlyInstanceOf(CappedScheduler.ActiveWorker.class);
		assertThat(worker2).isExactlyInstanceOf(CappedScheduler.ActiveWorker.class);
		assertThat(worker3).isExactlyInstanceOf(CappedScheduler.ActiveWorker.class);
		assertThat(worker4).isExactlyInstanceOf(CappedScheduler.ActiveWorker.class);
		assertThat(worker5).isExactlyInstanceOf(CappedScheduler.DeferredWorker.class);

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
		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());
		Scheduler.Worker worker3 = autoCleanup(s.createWorker());
		Scheduler.Worker worker4 = autoCleanup(s.createWorker());

		Disposable extraDirect1 = autoCleanup(s.schedule(() -> { }));
		Disposable extraDirect2 = autoCleanup(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		Disposable extraDirect3 = autoCleanup(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));

		assertThat(extraDirect1)
				.as("extraDirect1")
				.isNotSameAs(extraDirect2)
				.isNotSameAs(extraDirect3)
				.isInstanceOf(CappedScheduler.DeferredDirect.class);

		assertThat(extraDirect2)
				.as("extraDirect2")
				.isNotSameAs(extraDirect1)
				.isNotSameAs(extraDirect3)
				.isInstanceOf(CappedScheduler.DeferredDirect.class);

		assertThat(extraDirect3)
				.as("extraDirect3")
				.isInstanceOf(CappedScheduler.DeferredDirect.class);
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
				.isThrownBy(() -> new CappedScheduler(1, Integer.MAX_VALUE,null, -1));
	}

	@Test
	public void negativeThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(-1, Integer.MAX_VALUE, null, 1));
	}

	@Test
	public void zeroThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(0, Integer.MAX_VALUE, null, 1));
	}

	@Test
	public void negativeTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(1, -1, null, 1));
	}

	@Test
	public void zeroTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(1, 0, null, 1));
	}

	@Test
	public void evictionForWorkerScheduling() {
		CappedScheduler s = autoCleanup(new CappedScheduler(2, Integer.MAX_VALUE, r -> new Thread(r, "eviction"), 1));

		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());
		Scheduler.Worker worker3 = autoCleanup(s.createWorker());

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
		CappedScheduler s = autoCleanup(new CappedScheduler(2, Integer.MAX_VALUE, r -> new Thread(r, "eviction"), 1));

		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());

		AtomicReference<String> taskRanIn = new AtomicReference<>();
		autoCleanup(s.schedule(() -> taskRanIn.set(Thread.currentThread().getName())));

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
		Scheduler scheduler = autoCleanup(new CappedScheduler(200, Integer.MAX_VALUE, r -> new Thread(r, "dequeueEviction"), 1));
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
		Scheduler s = autoCleanup(Schedulers.newCapped(4, "cappedUserThread", 10, false));
		Scheduler.Worker w = autoCleanup(s.createWorker());

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
		CappedScheduler s = autoCleanup(new reactor.core.scheduler.CappedScheduler(1, Integer.MAX_VALUE, Thread::new,10));
		Scheduler.Worker firstWorker = autoCleanup(s.createWorker());
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
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "regrowFromEviction", 1));
		Scheduler.Worker worker = scheduler.createWorker();
		worker.schedule(() -> {});

		List<CappedScheduler.CachedService> beforeEviction = new ArrayList<>(scheduler.allServices);
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

		Scheduler.Worker regrowWorker = autoCleanup(scheduler.createWorker());
		assertThat(regrowWorker).isInstanceOf(CappedScheduler.ActiveWorker.class);
		regrowWorker.schedule(() -> {});

		assertThat(scheduler.allServices)
				.as("after regrowth")
				.isNotEmpty()
				.hasSize(1)
				.doesNotContainAnyElementsOf(beforeEviction);
	}

	@Test
	public void taskCapIsSharedBetweenDirectAndIndirectDeferredScheduling() {
		CappedScheduler cappedScheduler = autoCleanup(new CappedScheduler(1, 9, Thread::new, 10));
		Scheduler.Worker activeWorker = autoCleanup(cappedScheduler.createWorker());
		Scheduler.Worker deferredWorker1 = cappedScheduler.createWorker();
		Scheduler.Worker deferredWorker2 = cappedScheduler.createWorker();

		//enqueue tasks in first deferred worker
		deferredWorker1.schedule(() -> {});
		deferredWorker1.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		deferredWorker1.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks in second deferred worker
		deferredWorker2.schedule(() -> {});
		deferredWorker2.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		deferredWorker2.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks directly on scheduler
		cappedScheduler.schedule(() -> {});
		cappedScheduler.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
		cappedScheduler.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS);

		//any attempt at scheduling more task should result in rejection
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(0);
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 immediate").isThrownBy(() -> deferredWorker1.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 delayed").isThrownBy(() -> deferredWorker1.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 periodic").isThrownBy(() -> deferredWorker1.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 immediate").isThrownBy(() -> deferredWorker2.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 delayed").isThrownBy(() -> deferredWorker2.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 periodic").isThrownBy(() -> deferredWorker2.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler immediate").isThrownBy(() -> cappedScheduler.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler delayed").isThrownBy(() -> cappedScheduler.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler periodic").isThrownBy(() -> cappedScheduler.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void taskCapResetWhenDirectDeferredTaskIsExecuted() {
		CappedScheduler cappedScheduler = autoCleanup(new CappedScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = cappedScheduler.createWorker();

		//enqueue tasks directly on scheduler
		cappedScheduler.schedule(() -> {});
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(0);

		activeWorker.dispose();
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenWorkerDeferredTaskIsExecuted() {
		CappedScheduler cappedScheduler = autoCleanup(new CappedScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = cappedScheduler.createWorker();
		Scheduler.Worker deferredWorker = cappedScheduler.createWorker();

		//enqueue tasks on deferred worker
		deferredWorker.schedule(() -> {});
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(0);

		activeWorker.dispose();
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenDirectDeferredTaskIsDisposed() {
		CappedScheduler cappedScheduler = autoCleanup(new CappedScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = cappedScheduler.createWorker();

		Disposable d = cappedScheduler.schedule(() -> {});
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(0);

		d.dispose();
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void taskCapResetWhenWorkerDeferredTaskIsDisposed() {
		CappedScheduler cappedScheduler = autoCleanup(new CappedScheduler(1, 1, Thread::new, 10));
		Scheduler.Worker activeWorker = cappedScheduler.createWorker();
		Scheduler.Worker deferredWorker = cappedScheduler.createWorker();

		//enqueue tasks on deferred worker
		Disposable d = deferredWorker.schedule(() -> {});
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(0);

		d.dispose();
		assertThat(cappedScheduler.remainingDeferredTasks).isEqualTo(1);
	}

	@Test
	public void deferredWorkerTasksEventuallyExecuted() {
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		Scheduler.Worker activeWorker = autoCleanup(scheduler.createWorker());
		Scheduler.Worker deferredWorker = autoCleanup(scheduler.createWorker());

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
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		Scheduler.Worker activeWorker = autoCleanup(scheduler.createWorker());

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
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		Scheduler.Worker activeWorker = autoCleanup(scheduler.createWorker());
		Scheduler.Worker deferredWorker = autoCleanup(scheduler.createWorker());

		assertThat(scheduler.deferredFacades).as("before dispose")
		                                     .hasSize(1)
		                                     .containsExactly((CappedScheduler.DeferredFacade) deferredWorker);

		deferredWorker.dispose();
		assertThat(scheduler.deferredFacades).as("after dispose").isEmpty();
	}

	@Test
	public void deferredDirectDisposalRemovesFromFacadeQueue() {
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		Scheduler.Worker activeWorker = autoCleanup(scheduler.createWorker());
		Disposable deferredDirect = scheduler.schedule(() -> {});

		assertThat(scheduler.deferredFacades).as("before dispose")
		                                     .hasSize(1)
		                                     .containsExactly((CappedScheduler.DeferredFacade) deferredDirect);

		deferredDirect.dispose();
		assertThat(scheduler.deferredFacades).as("after dispose").isEmpty();
	}

	@Test
	public void deferredWorkerSetServiceIgnoredIfDisposed() {
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		CappedScheduler.ActiveWorker activeWorker = (CappedScheduler.ActiveWorker) autoCleanup(scheduler.createWorker());
		CappedScheduler.DeferredWorker deferredWorker = (CappedScheduler.DeferredWorker) autoCleanup(scheduler.createWorker());

		deferredWorker.dispose();

		assertThat(scheduler.idleServicesWithExpiry).isEmpty();
		deferredWorker.setService(autoCleanup(new CappedScheduler.CachedService(scheduler)));

		assertThat(scheduler.idleServicesWithExpiry).hasSize(1);
	}

	@Test
	public void deferredDirectSetServiceIgnoredIfDisposed() {
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		CappedScheduler.ActiveWorker activeWorker = (CappedScheduler.ActiveWorker) autoCleanup(scheduler.createWorker());
		CappedScheduler.DeferredDirect deferredDirect = (CappedScheduler.DeferredDirect) autoCleanup(scheduler.schedule(() -> {}));

		deferredDirect.dispose();

		assertThat(scheduler.idleServicesWithExpiry).isEmpty();
		deferredDirect.setService(autoCleanup(new CappedScheduler.CachedService(scheduler)));

		assertThat(scheduler.idleServicesWithExpiry).hasSize(1);
	}

	@Test
	public void deferredWorkerRejectsTasksAfterBeingDisposed() {
		CappedScheduler scheduler = autoCleanup((CappedScheduler) Schedulers.newCapped(1, "test"));
		CappedScheduler.DeferredWorker deferredWorker = new CappedScheduler.DeferredWorker(scheduler);
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
	public void defaultCappedConfigurationIsConsistentWithJavadoc() {
		Schedulers.CachedScheduler cachedCapped = (Schedulers.CachedScheduler) Schedulers.capped();
		CappedScheduler capped = (CappedScheduler) cachedCapped.cached;

		//10 x number of CPUs
		assertThat(capped.threadCap)
				.as("default capped size")
				.isEqualTo(Schedulers.DEFAULT_CAPPED_SIZE)
				.isEqualTo(Runtime.getRuntime().availableProcessors() * 10);

		//60s TTL
		assertThat(capped.ttlSeconds)
				.as("default TTL")
				.isEqualTo(CappedScheduler.DEFAULT_TTL_SECONDS)
				.isEqualTo(60);

		//unbounded task queueing
		assertThat(capped.deferredTaskCap)
				.as("default unbounded task queueing")
				.isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = autoCleanup(Schedulers.newCapped(1, "scanName", 3));
		Scheduler withBasicFactory = autoCleanup(Schedulers.newCapped(1, Thread::new, 3));
		Scheduler withTaskCap = autoCleanup(Schedulers.newHardCapped(1, 123, Thread::new, 3));
		Scheduler cached = Schedulers.capped();

		Scheduler.Worker workerWithNamedFactory = autoCleanup(withNamedFactory.createWorker());
		Scheduler.Worker deferredWorkerWithNamedFactory = autoCleanup(withNamedFactory.createWorker());
		Scheduler.Worker workerWithBasicFactory = autoCleanup(withBasicFactory.createWorker());
		Scheduler.Worker deferredWorkerWithBasicFactory = autoCleanup(withBasicFactory.createWorker());

		assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
				.as("withNamedFactory")
				.isEqualTo("capped(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s)");

		assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
				.as("withBasicFactory")
				.isEqualTo("capped(maxThreads=1,maxTaskQueued=unbounded,ttl=3s)");

		assertThat(Scannable.from(withTaskCap).scan(Scannable.Attr.NAME))
				.as("withTaskCap")
				.isEqualTo("capped(maxThreads=1,maxTaskQueued=123,ttl=3s)");

		assertThat(cached)
				.as("capped() is cached")
				.is(SchedulersTest.CACHED_SCHEDULER);
		assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
				.as("default capped()")
				.isEqualTo("Schedulers.capped()");

		assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("workerWithNamedFactory")
				.isEqualTo("capped(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s).worker");

		assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("workerWithBasicFactory")
				.isEqualTo("capped(maxThreads=1,maxTaskQueued=unbounded,ttl=3s).worker");

		assertThat(Scannable.from(deferredWorkerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithNamedFactory")
				.isEqualTo("capped(\"scanName\",maxThreads=1,maxTaskQueued=unbounded,ttl=3s).deferredWorker");

		assertThat(Scannable.from(deferredWorkerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithBasicFactory")
				.isEqualTo("capped(maxThreads=1,maxTaskQueued=unbounded,ttl=3s).deferredWorker");
	}

	@Test
	public void scanCapacity() {
		Scheduler scheduler = autoCleanup(Schedulers.newCapped(1, Thread::new, 2));
		Scheduler.Worker activeWorker = autoCleanup(scheduler.createWorker());
		Scheduler.Worker deferredWorker = autoCleanup(scheduler.createWorker());

		//smoke test that second worker is a DeferredWorker
		assertThat(deferredWorker).as("check second worker is deferred").isExactlyInstanceOf(CappedScheduler.DeferredWorker.class);

		assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler capped").isEqualTo(1);
		assertThat(Scannable.from(activeWorker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(1);
		assertThat(Scannable.from(deferredWorker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void scanDeferredDirect() {
		CappedScheduler parent = autoCleanup(new CappedScheduler(3, 10, Thread::new, 60));
		CappedScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);

		CappedScheduler.DeferredDirect deferredDirect = new CappedScheduler.DeferredDirect(() -> {}, 10, 10, TimeUnit.MILLISECONDS, parent);

		assertThat(deferredDirect.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
		assertThat(deferredDirect.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isFalse();

		assertThat(deferredDirect.scan(Scannable.Attr.NAME)).as("NAME").isEqualTo(parent.toString() + ".deferredDirect");

		assertThat(deferredDirect.scan(Scannable.Attr.CAPACITY)).as("CAPACITY").isOne();
		assertThat(deferredDirect.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);

		//BUFFERED 1 as long as not realized
		assertThat(deferredDirect.scan(Scannable.Attr.BUFFERED)).as("BUFFERED").isOne();
		CappedScheduler.CachedService cachedService = autoCleanup(new CappedScheduler.CachedService(parent));
		deferredDirect.set(cachedService);
		assertThat(deferredDirect.scan(Scannable.Attr.BUFFERED)).as("BUFFERED with CachedService").isZero();

		deferredDirect.dispose();
		assertThat(deferredDirect.scan(Scannable.Attr.TERMINATED)).as("TERMINATED once disposed").isTrue();
		assertThat(deferredDirect.scan(Scannable.Attr.CANCELLED)).as("CANCELLED once disposed").isTrue();
	}

	@Test
	public void scanDeferredWorker() {
		CappedScheduler parent = autoCleanup(new CappedScheduler(3, 10, Thread::new, 60));
		CappedScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);

		CappedScheduler.DeferredWorker deferredWorker = new CappedScheduler.DeferredWorker(parent);

		assertThat(deferredWorker.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
		assertThat(deferredWorker.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isFalse();
		assertThat(deferredWorker.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(deferredWorker.scan(Scannable.Attr.NAME)).as("NAME").isEqualTo(parent.toString() + ".deferredWorker");

		//capacity depends on parent's remaining tasks
		assertThat(deferredWorker.scan(Scannable.Attr.CAPACITY)).as("CAPACITY").isEqualTo(9);
		CappedScheduler.REMAINING_DEFERRED_TASKS.decrementAndGet(parent);
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
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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.pivovarit.function.ThrowingRunnable;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
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
public class CappedSchedulerTest extends AbstractSchedulerTest {

	private static final Logger LOGGER = Loggers.getLogger(CappedSchedulerTest.class);

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
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
	public void whenCapReachedDirectTasksAreRejected() throws InterruptedException {
		Scheduler s = schedulerNotCached();
		//reach the cap of workers
		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());
		Scheduler.Worker worker3 = autoCleanup(s.createWorker());
		Scheduler.Worker worker4 = autoCleanup(s.createWorker());

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}));

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
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
				.isThrownBy(() -> new CappedScheduler(1,null, -1));
	}

	@Test
	public void negativeCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(-1, null, 1));
	}

	@Test
	public void zeroCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CappedScheduler(0,null, 1));
	}

	@Test
	public void eviction() {
		CappedScheduler s = autoCleanup(new CappedScheduler(2, r -> new Thread(r, "eviction"), 1));

		Scheduler.Worker worker1 = autoCleanup(s.createWorker());
		Scheduler.Worker worker2 = autoCleanup(s.createWorker());
		Scheduler.Worker worker3 = autoCleanup(s.createWorker());

		assertThat(s.allServices).as("3 workers equals 2 executors").hasSize(2);
		assertThat(s.deferredWorkers).as("3 workers equals 1 deferred").hasSize(1);
		assertThat(s.idleServicesWithExpiry).as("no worker expiry").isEmpty();

		worker1.dispose();
		assertThat(s.idleServicesWithExpiry).as("deferred worker activated: no expiry").isEmpty();
		assertThat(s.deferredWorkers).as("deferred worker activated: no deferred").isEmpty();

		worker2.dispose();
		worker3.dispose();

		Awaitility.with()
		          .pollInterval(50, TimeUnit.MILLISECONDS)
		          .await()
		          //the evictor in the background can and does have a shift, but not more than 1s
		          .between(1, TimeUnit.SECONDS, 2500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> {
		          	assertThat(s.allServices).hasSize(0);
		          	assertThat(s.deferredWorkers).hasSize(0);
		          });
	}

	@Test
	public void lifoEviction() throws InterruptedException {
		Scheduler scheduler = autoCleanup(new CappedScheduler(200, r -> new Thread(r, "dequeueEviction"), 1));
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
		          .untilAsserted(() -> {
			          Thread[] tarray = new Thread[0];
			          for(;;) {
				          tarray = new Thread[Thread.activeCount()];
				          int dumped = Thread.enumerate(tarray);
				          if (dumped <= tarray.length) {
					          break;
				          }
			          }
			          assertThat(Arrays.stream(tarray).map(Thread::getName))
					          .doesNotContain(threadName.get());
				    });
	}

	@Test
	public void deferredWorkerDisposedEarly() {
		CappedScheduler s = autoCleanup(new reactor.core.scheduler.CappedScheduler(1, Thread::new,10));
		Scheduler.Worker firstWorker = autoCleanup(s.createWorker());
		Scheduler.Worker worker = s.createWorker();

		assertThat(s.deferredWorkers).as("deferred workers before inverted dispose").hasSize(1);
		assertThat(s.idleServicesWithExpiry).as("threads before inverted dispose").isEmpty();

		worker.dispose();
		firstWorker.dispose();

		assertThat(s.deferredWorkers).as("deferred workers after inverted dispose").isEmpty();
		assertThat(s.idleServicesWithExpiry).as("threads after inverted dispose").hasSize(1);
	}

	@Test
	public void defaultCappedConfigurationIsConsistentWithJavadoc() {
		Schedulers.CachedScheduler cachedCapped = (Schedulers.CachedScheduler) Schedulers.capped();
		CappedScheduler capped = (CappedScheduler) cachedCapped.cached;

		//10 x number of CPUs
		assertThat(capped.cap)
				.as("default capped size")
				.isEqualTo(Schedulers.DEFAULT_CAPPED_SIZE)
				.isEqualTo(Runtime.getRuntime().availableProcessors() * 10);

		//60s TTL
		assertThat(capped.ttlSeconds)
				.as("default TTL")
				.isEqualTo(CappedScheduler.DEFAULT_TTL_SECONDS)
				.isEqualTo(60);
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = autoCleanup(Schedulers.newCapped(1, "scanName", 3));
		Scheduler withBasicFactory = autoCleanup(Schedulers.newCapped(1, Thread::new, 3));
		Scheduler cached = Schedulers.capped();

		Scheduler.Worker workerWithNamedFactory = autoCleanup(withNamedFactory.createWorker());
		Scheduler.Worker deferredWorkerWithNamedFactory = autoCleanup(withNamedFactory.createWorker());
		Scheduler.Worker workerWithBasicFactory = autoCleanup(withBasicFactory.createWorker());
		Scheduler.Worker deferredWorkerWithBasicFactory = autoCleanup(withBasicFactory.createWorker());

		assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
				.as("withNamedFactory")
				.isEqualTo("capped(\"scanName\",1,3s)");

		assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
				.as("withBasicFactory")
				.isEqualTo("capped(1,3s)");

		assertThat(cached)
				.as("capped() is cached")
				.is(SchedulersTest.CACHED_SCHEDULER);
		assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
				.as("default capped()")
				.isEqualTo("capped(\"capped\")");

		assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("workerWithNamedFactory")
				.isEqualTo("capped(\"scanName\",1,3s).worker");

		assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("workerWithBasicFactory")
				.isEqualTo("capped(1,3s).worker");

		assertThat(Scannable.from(deferredWorkerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithNamedFactory")
				.isEqualTo("capped(\"scanName\",1,3s).deferredWorker");

		assertThat(Scannable.from(deferredWorkerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("deferredWorkerWithBasicFactory")
				.isEqualTo("capped(1,3s).deferredWorker");
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
}
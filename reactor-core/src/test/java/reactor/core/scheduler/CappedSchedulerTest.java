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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
		//TODO replace with Schedulers factory
		return autoCleanup(
				new CappedScheduler(
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
	public void scanName() {
		Scheduler withNamedFactory = Schedulers.newCapped(3, "scanName", 1);
		Scheduler withBasicFactory = Schedulers.newCapped(3, Thread::new, 1);
		//TODO add test for the cached version when there is a default capped()
//		Scheduler cached = Schedulers.elastic();

		Scheduler.Worker workerWithNamedFactory = withNamedFactory.createWorker();
		Scheduler.Worker workerWithBasicFactory = withBasicFactory.createWorker();

		try {
			assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
					.as("withNamedFactory")
					.isEqualTo("capped(\"scanName\")");

			assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
					.as("withBasicFactory")
					.isEqualTo("capped()");

			//TODO reactivate assertions when there is a default capped()
//			assertThat(cached)
//					.as("capped() is cached")
//					.is(SchedulersTest.CACHED_SCHEDULER);
//			assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
//					.as("default capped()")
//					.isEqualTo("capped(\"capped\")");

			assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
					.as("workerWithNamedFactory")
					.isEqualTo("capped(\"scanName\").worker");

			assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
					.as("workerWithBasicFactory")
					.isEqualTo("capped().worker");
		}
		finally {
			withNamedFactory.dispose();
			withBasicFactory.dispose();
			workerWithNamedFactory.dispose();
			workerWithBasicFactory.dispose();
		}
	}

	@Test
	public void scanCapacity() {
		Scheduler scheduler = Schedulers.newCapped(2, Thread::new, 2);
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler capped").isEqualTo(2);
			assertThat(Scannable.from(worker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(1);
		}
		finally {
			worker.dispose();
			scheduler.dispose();
		}
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
//
//	@Test
//	public void doesntRecycleWhileRunningAfterDisposed() throws Exception {
//		Scheduler s = Schedulers.newElastic("test-recycle");
//		((ElasticScheduler)s).evictor.shutdownNow();
//
//		try {
//			AtomicBoolean stop = new AtomicBoolean(false);
//			CountDownLatch started = new CountDownLatch(1);
//			Disposable d = s.schedule(() -> {
//				started.countDown();
//				// simulate uninterruptible computation
//				for (;;) {
//					if (stop.get()) {
//						break;
//					}
//				}
//			});
//			assertThat(started.await(10, TimeUnit.SECONDS)).as("latch timeout").isTrue();
//			d.dispose();
//
//			Thread.sleep(100);
//			assertThat(((ElasticScheduler)s).cache).isEmpty();
//
//			stop.set(true);
//
//			Thread.sleep(100);
//			assertThat(((ElasticScheduler)s).cache.size()).isEqualTo(1);
//		}
//		finally {
//			s.dispose();
//		}
//	}
//
//	@Test
//	public void recycleOnce() throws Exception {
//		Scheduler s = Schedulers.newElastic("test-recycle");
//		((ElasticScheduler)s).evictor.shutdownNow();
//
//		try {
//			Disposable d = s.schedule(() -> {
//				try {
//					Thread.sleep(10000);
//				}
//				catch (InterruptedException e) {
//					Thread.currentThread().interrupt();
//				}
//			});
//
//			// Dispose twice to test that the executor is returned to the pool only once
//			d.dispose();
//			d.dispose();
//
//			Thread.sleep(100);
//			assertThat(((ElasticScheduler)s).cache.size()).isEqualTo(1);
//		}
//		finally {
//			s.dispose();
//		}
//	}
}
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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.pivovarit.function.ThrowingRunnable;
import com.pivovarit.function.ThrowingSupplier;
import org.awaitility.Awaitility;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.BoundedElasticScheduler.BoundedScheduledExecutorService;
import reactor.core.scheduler.BoundedElasticScheduler.BoundedServices;
import reactor.core.scheduler.BoundedElasticScheduler.BoundedState;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
public class BoundedElasticSchedulerTest extends AbstractSchedulerTest {

	private static final Logger LOGGER = Loggers.getLogger(BoundedElasticSchedulerTest.class);
	private static final AtomicLong COUNTER = new AtomicLong();

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

	@AfterAll
	public static void dumpThreads() {
		LOGGER.debug("Remaining threads after test class:");
		LOGGER.debug(dumpThreadNames().collect(Collectors.joining(", ")));
	}

	//note: blocking behavior is also tested in BoundedElasticSchedulerBlockhoundTest (separate sourceset)

	@Override
	protected BoundedElasticScheduler scheduler() {
		BoundedElasticScheduler scheduler =
				afterTest.autoDispose(new BoundedElasticScheduler(4,
						Integer.MAX_VALUE,
						new ReactorThreadFactory("boundedElasticSchedulerTest",
								COUNTER,
								false,
								false,
								Schedulers::defaultUncaughtException),
						10));
		scheduler.start();
		return scheduler;
	}

	@Test
	public void extraWorkersShareBackingExecutorAndBoundedState() throws InterruptedException {
		Scheduler s = schedulerNotCached();

		ExecutorServiceWorker worker1 = (ExecutorServiceWorker) afterTest.autoDispose(s.createWorker());
		ExecutorServiceWorker worker2 = (ExecutorServiceWorker) afterTest.autoDispose(s.createWorker());
		ExecutorServiceWorker worker3 = (ExecutorServiceWorker) afterTest.autoDispose(s.createWorker());
		ExecutorServiceWorker worker4 = (ExecutorServiceWorker) afterTest.autoDispose(s.createWorker());
		ExecutorServiceWorker worker5 = (ExecutorServiceWorker) afterTest.autoDispose(s.createWorker());

		assertThat(worker1.exec)
				.as("worker1")
				.isNotSameAs(worker2.exec)
				.isNotSameAs(worker3.exec)
				.isNotSameAs(worker4.exec)
				.isSameAs(worker5.exec);
		assertThat(worker2.exec)
				.as("worker2")
				.isNotSameAs(worker3.exec)
				.isNotSameAs(worker4.exec);
		assertThat(worker3.exec)
				.as("worker3")
				.isNotSameAs(worker4.exec);

		BoundedState worker1BoundedState = Scannable
				.from(worker1.disposables).inners()
				.findFirst()
				.map(o -> (BoundedState) o)
				.get();

		BoundedState worker5BoundedState = Scannable
				.from(worker5.disposables).inners()
				.findFirst()
				.map(o -> (BoundedState) o)
				.get();

		assertThat(worker1BoundedState)
				.as("w1 w5 same BoundedState in tasks")
				.isSameAs(worker5BoundedState);
	}

	@Test
	public void doubleSubscribeOn() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, Integer.MAX_VALUE,
				new ReactorThreadFactory("subscriberElastic", new AtomicLong(), false, false, null), 60));
		scheduler.start();

		final Mono<Integer> integerMono = Mono
				.fromSupplier(() -> 1)
				.subscribeOn(scheduler)
				.subscribeOn(scheduler);

		integerMono.block(Duration.ofSeconds(3));
	}

	@Test
	public void testLargeNumberOfWorkers() throws InterruptedException {
		final int maxThreads = 3;
		final int maxQueue = 10;

		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(maxThreads, maxQueue,
				new ReactorThreadFactory("largeNumberOfWorkers", new AtomicLong(), false, false, null),
				1));
		scheduler.start();

		CountDownLatch latch = new CountDownLatch(1);

		Flux<String> flux = Flux
				.range(1, maxQueue)
				.map(v -> {
					try {
						Thread.sleep(100);
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
					return "value" + v;
				})
				.subscribeOn(scheduler)
				.doOnNext(v -> System.out.println("published " + v));

		for (int i = 0; i < maxQueue * maxThreads - 1; i++) {
			flux = flux.publishOn(scheduler, false, 1 + i % 2);
		}

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		flux.doFinally(sig -> latch.countDown())
		    .subscribe(s -> {}, errorRef::set);

		assertThat(scheduler.estimateSize()).as("all 3 threads created").isEqualTo(3);

		assertThat(latch.await(11, TimeUnit.SECONDS)).as("completed").isTrue();

		assertThat(errorRef).as("no error").hasValue(null);

		Awaitility.with().pollDelay(1, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
		          .await().atMost(3, TimeUnit.SECONDS)
		          .untilAsserted(() -> assertThat(scheduler.estimateSize()).as("post eviction").isZero());
	}

	@Test
	public void testSmallTaskCapacityReached() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 2,
				new ReactorThreadFactory("testSmallTaskCapacityReached", new AtomicLong(), false, false, null), 60));
		scheduler.start();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() ->
						Flux.interval(Duration.ofSeconds(1), scheduler)
						    .doOnNext(ignored -> System.out.println("emitted"))
						    .publishOn(scheduler)
						    .publishOn(scheduler)
						    .publishOn(scheduler)
						    .publishOn(scheduler)
						    .doOnNext(ignored -> System.out.println("published"))
						    .blockFirst(Duration.ofSeconds(2))
				)
				.withMessage("Task capacity of bounded elastic scheduler reached while scheduling 1 tasks (3/2)");
	}

	@Test
	public void testSmallTaskCapacityJustEnough() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 2,
				new ReactorThreadFactory("testSmallTaskCapacityJustEnough", new AtomicLong(), false, false, null), 60));
		scheduler.start();

		assertThat(Flux.interval(Duration.ofSeconds(1), scheduler)
		               .doOnNext(ignored -> System.out.println("emitted"))
		               .publishOn(scheduler)
		               .doOnNext(ignored -> System.out.println("published"))
		               .blockFirst(Duration.ofSeconds(2))
		).isEqualTo(0);
	}

	@Test
	public void TODO_TEST_MUTUALLY_DELAYING_TASKS() {
//		AtomicInteger taskRun = new Atomic Integer();
//		worker1.schedule(() -> {});
//		worker2.schedule(() -> {});
//		worker3.schedule(() -> {});
//		worker4.schedule(() -> {});
//		Disposable periodicDeferredTask = worker5.schedulePeriodically(taskRun::incrementAndGet, 0L, 100, TimeUnit.MILLISECONDS);
//
//		Awaitility.with().pollDelay(100, TimeUnit.MILLISECONDS)
//		          .untilAsserted(() -> assertThat(taskRun).as("task held due to worker cap").hasValue(0));
//
//		worker1.dispose(); //should trigger work stealing of worker5
//
//		Awaitility.waitAtMost(250, TimeUnit.MILLISECONDS)
//		          .untilAsserted(() -> assertThat(taskRun).as("task running periodically").hasValue(3));
//
//		periodicDeferredTask.dispose();
//
//		int onceCancelled = taskRun.get();
//		Awaitility.with()
//		          .pollDelay(200, TimeUnit.MILLISECONDS)
//		          .untilAsserted(() -> assertThat(taskRun).as("task has stopped").hasValue(onceCancelled));
	}

	@Test
	public void whenCapReachedPicksLeastBusyExecutor() throws InterruptedException {
		BoundedElasticScheduler s = scheduler();
		//reach the cap of workers
		BoundedState state1 = afterTest.autoDispose(s.boundedServices.pick());
		BoundedState state2 = afterTest.autoDispose(s.boundedServices.pick());
		BoundedState state3 = afterTest.autoDispose(s.boundedServices.pick());
		BoundedState state4 = afterTest.autoDispose(s.boundedServices.pick());

		assertThat(new HashSet<>(Arrays.asList(state1, state2, state3, state4))).as("4 distinct").hasSize(4);
		//cheat to make some look like more busy
		s.boundedServices.busyQueue.remove(state1);
		s.boundedServices.busyQueue.remove(state2);
		s.boundedServices.busyQueue.remove(state3);
		state1.markPicked();
		state1.markPicked();
		state1.markPicked();
		state2.markPicked();
		state2.markPicked();
		state3.markPicked();
		s.boundedServices.busyQueue.addAll(Arrays.asList(state1, state2, state3));

		assertThat(s.boundedServices.pick()).as("picked least busy state4").isSameAs(state4);
		//at this point state4 and state3 both are backing 1
		assertThat(Arrays.asList(s.boundedServices.pick(), s.boundedServices.pick()))
				.as("next 2 picks picked state4 and state3")
				.containsExactlyInAnyOrder(state4, state3);
	}

	@Test
	public void startNoOpIfStarted() {
		BoundedElasticScheduler s = scheduler();
		//need a first call to `start()` after construction
		BoundedServices servicesBefore = s.boundedServices;

		s.start();
		s.start();
		s.start();

		assertThat(s.boundedServices).isSameAs(servicesBefore);
	}

	@Test
	public void restartSupported() {
		BoundedElasticScheduler s = scheduler();
		s.dispose();
		BoundedServices servicesBefore = s.boundedServices;

		assertThat(servicesBefore).as("SHUTDOWN").isSameAs(BoundedElasticScheduler.SHUTDOWN);

		s.start();

		assertThat(s.boundedServices)
				.isNotSameAs(servicesBefore)
				.hasValue(0);
	}

	// below tests similar to ElasticScheduler
	@Test
	public void negativeTtl() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, Integer.MAX_VALUE,null, -1))
				.withMessage("TTL must be strictly positive, was -1000ms");
	}

	@Test
	public void zeroTtl() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, Integer.MAX_VALUE,null, 0))
				.withMessage("TTL must be strictly positive, was 0ms");
	}

	@Test
	public void maximumTtl() {
		BoundedElasticScheduler s = new BoundedElasticScheduler(1, Integer.MAX_VALUE,null, Integer.MAX_VALUE);
		assertThat(s.ttlMillis).isEqualTo(Integer.MAX_VALUE * 1000L);
	}

	@Test
	public void negativeThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(-1, Integer.MAX_VALUE, null, 1))
				.withMessage("maxThreads must be strictly positive, was -1");
	}

	@Test
	public void zeroThreadCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(0, Integer.MAX_VALUE, null, 1))
				.withMessage("maxThreads must be strictly positive, was 0");
	}

	@Test
	public void negativeTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, -1, null, 1))
				.withMessage("maxTaskQueuedPerThread must be strictly positive, was -1");
	}

	@Test
	public void zeroTaskCap() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BoundedElasticScheduler(1, 0, null, 1))
				.withMessage("maxTaskQueuedPerThread must be strictly positive, was 0");
	}

	@Test
	public void evictionForWorkerScheduling() {
		MockUtils.VirtualClock clock = new MockUtils.VirtualClock(Instant.ofEpochMilli(1_000_000), ZoneId.systemDefault());
		BoundedElasticScheduler s = afterTest.autoDispose(new BoundedElasticScheduler(2, Integer.MAX_VALUE, r -> new Thread(r, "eviction"),
				60*1000, clock));
		s.start();
		BoundedServices services = s.boundedServices;

		Worker worker1 = afterTest.autoDispose(s.createWorker());

		assertThat(services).as("count worker 1").hasValue(1);
		assertThat(s.estimateSize()).as("non null size before workers 2 and 3").isEqualTo(1);

		Worker worker2 = afterTest.autoDispose(s.createWorker());
		Worker worker3 = afterTest.autoDispose(s.createWorker());

		assertThat(services).as("count worker 1 2 3").hasValue(2);
		assertThat(s.estimateSize()).as("3 workers equals 2 executors").isEqualTo(2);

		services.eviction();
		assertThat(s.estimateIdle()).as("not idle yet").isZero();

		clock.advanceTimeBy(Duration.ofMillis(1));
		worker1.dispose();
		worker2.dispose();
		worker3.dispose();
		clock.advanceTimeBy(Duration.ofMillis(10));
		services.eviction();

		assertThat(s.estimateIdle()).as("idle for 10 milliseconds").isEqualTo(2);

		clock.advanceTimeBy(Duration.ofMinutes(1));
		services.eviction();

		assertThat(s.estimateIdle()).as("idle for 1 minute and 10ms")
		                            .isEqualTo(s.estimateBusy())
		                            .isEqualTo(s.estimateSize())
		                            .isZero();
	}

	@Test
	public void lifoEvictionNoThreadRegrowth() throws InterruptedException {
		int otherThreads = Thread.activeCount(); //don't count the evictor at shutdown
		Set<String> preExistingEvictors = dumpThreadNames().filter(s -> s.startsWith("boundedElastic-evictor")).collect(Collectors.toSet());
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(200, Integer.MAX_VALUE,
				r -> new Thread(r, "dequeueEviction"), 1));
		scheduler.start();

		List<String> newEvictors = dumpThreadNames()
				.filter(s -> s.startsWith("boundedElastic-evictor"))
				.filter(s -> !preExistingEvictors.contains(s))
				.collect(Collectors.toList());
		assertThat(newEvictors).as("new evictors").hasSize(1);
		String newEvictor = newEvictors.get(0);

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

			int[] threadCountTrend = new int[fastCount + 1];
			int threadCountChange = 1;

			int oldActive = 0;
			int activeAtBeginning = 0;
			for (int i = 0; i < fastCount; i++) {
				Mono.just(i)
				    .subscribeOn(scheduler)
				    .doFinally(sig -> latch.countDown())
				    .subscribe();

				if (i == 0) {
					activeAtBeginning = Math.max(0, Thread.activeCount() - otherThreads);
					threadCountTrend[0] = activeAtBeginning;
					oldActive = activeAtBeginning;
					LOGGER.debug("{} threads active in round 1/{}", activeAtBeginning, fastCount);
				}
				else {
					int newActive = Math.max(0, Thread.activeCount() - otherThreads);
					if (oldActive != newActive) {
						threadCountTrend[threadCountChange++] = newActive;
						oldActive = newActive;
						LOGGER.debug("{} threads active in round {}/{}", newActive, i + 1, fastCount);
					}
				}
				Thread.sleep(fastSleep);
			}

			assertThat(scheduler.estimateBusy()).as("busy at end of loop").isZero();
			assertThat(threadCountTrend).as("no thread regrowth").isSortedAccordingTo(Comparator.reverseOrder());
			assertThat(dumpThreadNames().filter(name -> name.contains("dequeueEviction")).count())
					.as("at most 1 worker at end").isLessThanOrEqualTo(1);

			System.out.println(Arrays.toString(Arrays.copyOf(threadCountTrend, threadCountChange)));
		}
		finally {
			scheduler.dispose();
			Thread.sleep(100);
			final long postShutdown = dumpThreadNames().filter(name -> name.contains("dequeueEviction")).count();
			LOGGER.info("{} worker threads active post shutdown", postShutdown);
			assertThat(postShutdown)
					.as("post shutdown")
					.withFailMessage("worker thread count after shutdown is not zero. threads: %s", Thread.getAllStackTraces().keySet())
					.isNotPositive();
			assertThat(dumpThreadNames())
					.as("current evictor %s shutdown", newEvictor)
					.doesNotContain(newEvictor);
		}
	}

	@Test
	public void userWorkerShutdownBySchedulerDisposal() throws InterruptedException {
		Scheduler s = afterTest.autoDispose(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "boundedElasticUserThread", 10, false));
		Worker w = afterTest.autoDispose(s.createWorker());

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
	public void regrowFromEviction() {
		MockUtils.VirtualClock virtualClock = new MockUtils.VirtualClock();
		BoundedElasticScheduler
				scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, Integer.MAX_VALUE, r -> new Thread(r, "regrowFromEviction"),
				1000, virtualClock));
		scheduler.start();

		Worker worker = scheduler.createWorker();

		List<BoundedState> beforeEviction = new ArrayList<>(scheduler.boundedServices.busyQueue);

		assertThat(scheduler.estimateSize())
				.as("before eviction")
				.isEqualTo(scheduler.estimateBusy())
				.isEqualTo(beforeEviction.size())
				.isEqualTo(1);

		worker.dispose();
		assertThat(scheduler.estimateSize())
				.as("once disposed")
				.isEqualTo(scheduler.estimateIdle())
				.isEqualTo(1);

		//simulate an eviction 1s in the future
		virtualClock.advanceTimeBy(Duration.ofSeconds(1));
		scheduler.boundedServices.eviction();

		assertThat(scheduler.estimateSize())
				.as("after eviction")
				.isEqualTo(scheduler.estimateIdle())
				.isEqualTo(scheduler.estimateBusy())
				.isZero();

		afterTest.autoDispose(scheduler.createWorker());
		assertThat(scheduler.boundedServices.busyQueue)
				.as("after regrowth")
				.isNotEmpty()
				.hasSize(1)
				.doesNotContainAnyElementsOf(beforeEviction);
	}

	@Test
	public void taskCapIsOnExecutorAndNotWorker() {
		BoundedElasticScheduler
				boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 9, Thread::new, 10));
		boundedElasticScheduler.start();

		Worker worker1 = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		Worker worker2 = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		Worker worker3 = afterTest.autoDispose(boundedElasticScheduler.createWorker());

		//schedule tasks for second and third workers as well as directly on scheduler to show worker1 is still impacted
		worker2.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		worker2.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		worker2.schedulePeriodically(() -> {}, 1000, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks in second deferred worker
		worker3.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		worker3.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		worker3.schedulePeriodically(() -> {}, 1000, 100, TimeUnit.MILLISECONDS);

		//enqueue tasks on scheduler directly
		boundedElasticScheduler.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		boundedElasticScheduler.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
		boundedElasticScheduler.schedulePeriodically(() -> {}, 1000, 100, TimeUnit.MILLISECONDS);

		//any attempt at scheduling more task should result in rejection
		ScheduledThreadPoolExecutor threadPoolExecutor = (ScheduledThreadPoolExecutor) boundedElasticScheduler.boundedServices.busyQueue.peek().executor;
		assertThat(threadPoolExecutor.getQueue().size()).as("queue full").isEqualTo(9);
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 immediate").isThrownBy(() -> worker1.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 delayed").isThrownBy(() -> worker1.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker1 periodic").isThrownBy(() -> worker1.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 immediate").isThrownBy(() -> worker2.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 delayed").isThrownBy(() -> worker2.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("worker2 periodic").isThrownBy(() -> worker2.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler immediate").isThrownBy(() -> boundedElasticScheduler.schedule(() -> {}));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler delayed").isThrownBy(() -> boundedElasticScheduler.schedule(() -> {}, 100, TimeUnit.MILLISECONDS));
		assertThatExceptionOfType(RejectedExecutionException.class).as("scheduler periodic").isThrownBy(() -> boundedElasticScheduler.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void estimateRemainingTaskCapacityIsSumOfWorkers() {
		//3 workers
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(3, 5, Thread::new, 10));
		boundedElasticScheduler.start();

		afterTest.autoDispose(boundedElasticScheduler.createWorker());
		afterTest.autoDispose(boundedElasticScheduler.createWorker());
		afterTest.autoDispose(boundedElasticScheduler.createWorker());

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("precise capacity").isEqualTo(3*5);
	}

	@Test
	public void estimateRemainingTaskCapacityWithSomeUnobservableWorkers() {
		//3 workers, 1 not observable
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(3, 5, Thread::new, 10));
		boundedElasticScheduler.start();

		afterTest.autoDispose(boundedElasticScheduler.createWorker());
		afterTest.autoDispose(boundedElasticScheduler.createWorker());
		boundedElasticScheduler.boundedServices.busyQueue.add(new BoundedState(boundedElasticScheduler.boundedServices, Executors.newSingleThreadScheduledExecutor()));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("partially computable capacity").isEqualTo(-1);
	}

	@Test
	public void estimateRemainingTaskCapacityWithUnobservableOnly() {
		//3 workers, 1 not observable
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(3, 5, Thread::new, 10));
		boundedElasticScheduler.start();

		boundedElasticScheduler.boundedServices.busyQueue.add(new BoundedState(boundedElasticScheduler.boundedServices, Executors.newSingleThreadScheduledExecutor()));
		boundedElasticScheduler.boundedServices.busyQueue.add(new BoundedState(boundedElasticScheduler.boundedServices, Executors.newSingleThreadScheduledExecutor()));
		boundedElasticScheduler.boundedServices.busyQueue.add(new BoundedState(boundedElasticScheduler.boundedServices, Executors.newSingleThreadScheduledExecutor()));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("non-computable capacity").isEqualTo(-1);
	}

	@Test
	public void estimateRemainingTaskCapacityResetWhenDirectTaskIsExecuted()
			throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean taskRan = new AtomicBoolean();
		//occupy the scheduler
		boundedElasticScheduler.schedule(() -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		Thread.sleep(10); //small window to start the first task
		//enqueue task on worker
		Disposable task = boundedElasticScheduler.schedule(() -> taskRan.set(true));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity when running").isZero();
		latch.countDown();
		Awaitility.await().untilTrue(taskRan);
		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity after run").isOne();
	}

	@Test
	public void estimateRemainingTaskCapacityResetWhenWorkerTaskIsExecuted()
			throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		Worker worker = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean taskRan = new AtomicBoolean();
		//occupy the scheduler
		worker.schedule(() -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		Thread.sleep(10); //small window to start the first task
		//enqueue task on worker
		Disposable task = worker.schedule(() -> taskRan.set(true));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity when running").isZero();
		latch.countDown();
		Awaitility.await().untilTrue(taskRan);
		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity after run").isOne();
	}

	@Test
	public void estimateRemainingTaskCapacityResetWhenDirectTaskIsDisposed()
			throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean taskRan = new AtomicBoolean();
		//occupy the scheduler
		boundedElasticScheduler.schedule(() -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				//expected to be interrupted
			}
		});
		Thread.sleep(10); //small window to start the first task
		//enqueue task on worker
		Disposable task = boundedElasticScheduler.schedule(() -> taskRan.set(true));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity when running").isZero();
		task.dispose();
		Awaitility.with().pollDelay(50, TimeUnit.MILLISECONDS)
		          .await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity())
				          .as("capacity after dispose").isOne());
	}

	@Test
	public void estimateRemainingTaskCapacityResetWhenWorkerTaskIsDisposed()
			throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		Worker worker = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean taskRan = new AtomicBoolean();
		//occupy the scheduler
		worker.schedule(() -> {
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				//expected to be interrupted
			}
		});
		Thread.sleep(10); //small window to start the first task
		//enqueue task on worker
		Disposable task = worker.schedule(() -> taskRan.set(true));

		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity when running").isZero();
		task.dispose();
		Awaitility.with().pollDelay(50, TimeUnit.MILLISECONDS)
				.await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity())
				          .as("capacity after dispose").isOne());
	}

	@Test
	public void taskPutInPendingQueueCanBeRemovedOnCancel() throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		Worker worker = afterTest.autoDispose(boundedElasticScheduler.createWorker());
		AtomicBoolean ranTask = new AtomicBoolean();
		CountDownLatch latch = new CountDownLatch(1);

		//block worker
		worker.schedule(() -> {
			try {
				latch.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		Thread.sleep(10); //small window to start the first task
		//enqueue task on worker
		Disposable task = worker.schedule(() -> ranTask.set(true));

		assertThat(ranTask).as("is pending execution").isFalse();

		Awaitility.with().pollInterval(50, TimeUnit.MILLISECONDS)
		          .await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity())
				          .as("queue full")
				          .isZero()
		          );

		task.dispose();

		Awaitility.with().pollInterval(50, TimeUnit.MILLISECONDS)
		          .await().atMost(100, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity())
				          .as("queue cleared").isOne());

		latch.countDown();
		Thread.sleep(100);
		assertThat(ranTask).as("not executed after latch countdown").isFalse();
	}

	@Test
	public void taskPutInPendingQueueIsEventuallyExecuted() throws InterruptedException {
		BoundedElasticScheduler boundedElasticScheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, Thread::new, 10));
		boundedElasticScheduler.start();

		Worker worker = afterTest.autoDispose(boundedElasticScheduler.createWorker());

		CountDownLatch latch = new CountDownLatch(1);

		//enqueue blocking task on worker
		worker.schedule(() -> {
			try {
				latch.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		Thread.sleep(10); //small window to start the first task
		AtomicBoolean ranSecond = new AtomicBoolean();
		Disposable task = worker.schedule(() -> ranSecond.set(true));

		assertThat(ranSecond).as("is pending execution").isFalse();
		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("queue full").isZero();

		latch.countDown();
		Awaitility.await().atMost(100, TimeUnit.MILLISECONDS)
		          .pollInterval(10, TimeUnit.MILLISECONDS)
		          .pollDelay(10, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(ranSecond)
				          .as("did run after task1 done")
				          .isTrue());
		assertThat(boundedElasticScheduler.estimateRemainingTaskCapacity()).as("capacity restored").isOne();
	}

	@Test
	public void workerRejectsTasksAfterBeingDisposed() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose((BoundedElasticScheduler) Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));
		Worker worker = scheduler.createWorker();
		worker.dispose();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> worker.schedule(() -> {}))
				.as("immediate schedule after dispose")
				.withMessage("Scheduler unavailable");

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> worker.schedule(() -> {}, 10, TimeUnit.MILLISECONDS))
				.as("delayed schedule after dispose")
				.withMessage("Scheduler unavailable");

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> worker.schedulePeriodically(() -> {}, 10, 10, TimeUnit.MILLISECONDS))
				.as("periodic schedule after dispose")
				.withMessage("Scheduler unavailable");
	}

	@Test
	public void blockingTasksWith100kLimit() throws InterruptedException {
		AtomicInteger taskDone = new AtomicInteger();
		AtomicInteger taskRejected = new AtomicInteger();

		int limit = 100_000;
		int workerCount = 70_000;

		CountDownLatch latch = new CountDownLatch(1);

		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(
				1, limit,
				"blockingTasksWith100kLimit"
		));
		Scheduler.Worker worker = afterTest.autoDispose(scheduler.createWorker());

		Runnable latchAndIncrement = () -> {
			try {
				latch.await(30, TimeUnit.SECONDS);
				taskDone.incrementAndGet();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		};

		//initial task that blocks the thread, causing other tasks to enter pending queue
		worker.schedule(() -> {
			try {
				latch.await(30, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		for (int i = 1; i <= limit + 10; i++) {
			if (i <= workerCount) {
				//larger subset of tasks are submitted to the worker
				worker.schedule(latchAndIncrement);
			}
			else if (i <= limit) {
				//smaller subset of tasks are submitted directly to the scheduler
				scheduler.schedule(latchAndIncrement);
			}
			else if (i % 2 == 0) {
				//half of over-limit tasks are submitted directly to the scheduler, half to worker
				assertThatExceptionOfType(RejectedExecutionException.class)
						.as("scheduler task " + i)
						.isThrownBy(() -> scheduler.schedule(latchAndIncrement));
				taskRejected.incrementAndGet();
			}
			else {
				//half of over-limit tasks are submitted directly to the scheduler, half to worker
				assertThatExceptionOfType(RejectedExecutionException.class)
						.as("worker task " + i)
						.isThrownBy(() -> worker.schedule(latchAndIncrement));
				taskRejected.incrementAndGet();
			}
		}

		assertThat(taskRejected).as("task rejected").hasValue(10);

		latch.countDown();

		Awaitility.with().pollInterval(50, TimeUnit.MILLISECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
		          .await().atMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(taskDone)
				          .as("all tasks done after blocking")
				          .hasValue(limit)
		          );
	}

	@Test
	public void delayedTasksWith100kLimit() {
		AtomicInteger taskDone = new AtomicInteger();
		AtomicInteger taskRejected = new AtomicInteger();

		int limit = 100_000;
		int workerCount = 70_000;


		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(
				1, limit,
				"delayedTasksWith100kLimit"
		));
		Scheduler.Worker worker = afterTest.autoDispose(scheduler.createWorker());

		//initial task that blocks the thread, causing other tasks to enter pending queue
		CountDownLatch latch = new CountDownLatch(1);
		worker.schedule(() -> {
			try {
				latch.await(30, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		for (int i = 0; i < limit + 10; i++) {
			try {
				if (i < workerCount) {
					//larger subset of tasks are submitted to the worker
					worker.schedule(taskDone::incrementAndGet, 100, TimeUnit.MILLISECONDS);
				}
				else if (i < limit) {
					//smaller subset of tasks are submitted directly to the scheduler
					scheduler.schedule(taskDone::incrementAndGet, 100, TimeUnit.MILLISECONDS);
				}
				else if (i % 2 == 0) {
					//half of over-limit tasks are submitted directly to the scheduler, half to worker
					assertThatExceptionOfType(RejectedExecutionException.class)
							.as("scheduler task " + i)
							.isThrownBy(() -> scheduler.schedule(taskDone::incrementAndGet, 100, TimeUnit.MILLISECONDS));
					taskRejected.incrementAndGet();
				}
				else {
					//half of over-limit tasks are submitted directly to the scheduler, half to worker
					assertThatExceptionOfType(RejectedExecutionException.class)
							.as("scheduler task " + i)
							.isThrownBy(() -> worker.schedule(taskDone::incrementAndGet, 100, TimeUnit.MILLISECONDS));
					taskRejected.incrementAndGet();
				}
			}
			catch (RejectedExecutionException ree) {
				taskRejected.incrementAndGet();
			}
		}
		//now that all are either scheduled or rejected, free up the thread
		latch.countDown();

		assertThat(taskRejected).as("task rejected").hasValue(10);

		Awaitility.with().pollInterval(50, TimeUnit.MILLISECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
		          .await().atMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(taskDone)
				          .as("all tasks done after delay")
				          .hasValue(limit)
		          );
	}

	@Test
	public void subscribeOnDisposesWorkerWhenCancelled() throws InterruptedException {
		AtomicInteger taskExecuted = new AtomicInteger();

		BoundedElasticScheduler bounded = afterTest.autoDispose(
				new BoundedElasticScheduler(1, 100, new ReactorThreadFactory("disposeMonoSubscribeOn", new AtomicLong(), false, false, null), 60));
		bounded.start();

		Disposable.Composite tasks = Disposables.composite();
		Runnable runnable = () -> {
			try {
				Thread.sleep(1000);
				taskExecuted.incrementAndGet();
			}
			catch (InterruptedException e) {
				//expecting tasks to be interrupted aka cancelled
			}
		};

		tasks.add(
				Mono.fromRunnable(runnable)
				    .subscribeOn(bounded)
				    .subscribe()
		);
		tasks.add(
				Mono.fromRunnable(runnable)
				    .hide()
				    .subscribeOn(bounded)
				    .subscribe()
		);
		tasks.add(
				Flux.just("foo")
				    .doOnNext(v -> runnable.run())
				    .subscribeOn(bounded)
				    .subscribe()
		);
		tasks.add(
				Flux.just("foo")
				    .hide()
				    .doOnNext(v -> runnable.run())
				    .subscribeOn(bounded)
				    .subscribe()
		);
		tasks.add(
				//test the FluxCallable (not ScalarCallable) case
				Mono.fromRunnable(runnable)
				    .flux()
				    .subscribeOn(bounded)
				    .subscribe()
		);

		assertThat(bounded.estimateBusy()).isPositive();

		tasks.dispose();

		Awaitility.waitAtMost(150, TimeUnit.MILLISECONDS).untilAsserted(() ->
				assertThat(bounded.estimateBusy()).isZero());
		assertThat(taskExecuted).hasValue(0);
	}

	@Test
	public void publishOnDisposesWorkerWhenCancelled() {
		AtomicInteger taskExecuted = new AtomicInteger();
		Function<String, Integer> sleepAndIncrement = v -> {
			try {
				Thread.sleep(1000);
				return taskExecuted.incrementAndGet();
			}
			catch (InterruptedException ie) {
				//swallow interruptions since these are expected as part of the worker cancelling.
				//propagating through onError triggers unnecessary logging: the subscriber is cancelled
				//and this results in onErrorDropped
				return 0;
			}
		};

		BoundedElasticScheduler bounded = afterTest.autoDispose(
				new BoundedElasticScheduler(1, 100, new ReactorThreadFactory("disposeMonoSubscribeOn", new AtomicLong(), false, false, null), 60));
		bounded.start();

		Disposable.Composite tasks = Disposables.composite();

		tasks.add(
				Mono.fromCallable(() -> "fromCallable fused")
				    .publishOn(bounded)
				    .map(sleepAndIncrement)
				    .subscribe()
		);
		tasks.add(
				Mono.fromCallable(() -> "fromCallable")
				    .hide()
				    .publishOn(bounded)
				    .map(sleepAndIncrement)
				    .subscribe()
		);
		tasks.add(
				Flux.just("just fused")
				    .publishOn(bounded)
				    .map(sleepAndIncrement)
				    .subscribe()
		);
		tasks.add(
				Flux.just("just")
				    .hide()
				    .publishOn(bounded)
				    .map(sleepAndIncrement)
				    .subscribe()
		);
		tasks.add(
				//test the FluxCallable (not ScalarCallable) case
				Mono.fromCallable(() -> "FluxCallable, not ScalarCallable")
				    .flux()
				    .publishOn(bounded)
				    .map(sleepAndIncrement)
				    .subscribe()
		);

		assertThat(bounded.estimateBusy()).isPositive();

		tasks.dispose();

		Awaitility.waitAtMost(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
				assertThat(bounded.estimateBusy()).isZero());
		assertThat(taskExecuted).hasValue(0);
	}

	@Test
	public void pickSetIdleRaceBusy() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, r -> new Thread(r, "test"),
				1000));
		scheduler.start();

		afterTest.autoDispose(scheduler.boundedServices.pick());

		for (int i = 0; i < 100_000; i++) {
			RaceTestUtils.race(
					() -> scheduler.boundedServices.pick().dispose(),
					() -> scheduler.boundedServices.pick().dispose()
			);
		}

		assertThat(scheduler.boundedServices.get()).as("state count").isOne();
		assertThat(scheduler.boundedServices.busyQueue.size() + scheduler.boundedServices.idleQueue.size()).as("busyOrIdle").isOne();

	}
	@Test
	public void pickSetIdleRaceIdle() {
		BoundedElasticScheduler scheduler = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, r -> new Thread(r, "test"),
				1000));
		scheduler.start();

		scheduler.boundedServices.pick().dispose();

		for (int i = 0; i < 100_000; i++) {
			RaceTestUtils.race(
					() -> scheduler.boundedServices.pick().dispose(),
					() -> scheduler.boundedServices.pick().dispose()
			);
		}

		assertThat(scheduler.boundedServices.busyQueue.size() + scheduler.boundedServices.idleQueue.size()).as("busyOrIdle").isOne();
	}

	//gh-1992 smoke test
	@Test
	public void gh1992() {
		final Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "test"));

		final Mono<Integer> integerMono = Mono
				.fromSupplier(ThrowingSupplier.sneaky(() -> {
					Thread.sleep(500);
					return 1;
				}))
				.subscribeOn(scheduler)
				.subscribeOn(scheduler);

		assertThat(integerMono.block(Duration.ofSeconds(1))).isEqualTo(1);
	}

	//gh-1973 smoke test
	@Test
	public void testGh1973() throws InterruptedException {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(3, 100000, "subscriberElastic", 600, true));
		LinkedList<MonoSink<String>> listeners = new LinkedList<>();
		List<Disposable> scheduled = new LinkedList<>();

		ExecutorService producer = startProducer(listeners);

		Consumer<MonoSink<String>> addListener = sink -> {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				LOGGER.trace("listener cancelled");
			}
			listeners.add(sink);
			sink.onDispose(() -> listeners.remove(sink));
		};

		for (int i = 0; i < 50; i++) {
			scheduled.add(
					Mono.create(addListener)
					    .subscribeOn(scheduler)
					    .subscribe(LOGGER::info)
			);
		}

		Thread.sleep(1000);
		scheduled.forEach(Disposable::dispose);
		Thread.sleep(1000);

		Mono.<String>create(sink -> {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				LOGGER.warn("last listener improperly cancelled");
			}
			listeners.add(sink);
			sink.onDispose(() -> listeners.remove(sink));
		})
		    .subscribeOn(scheduler)
		    .map(res -> res + " the end")
		    .doOnNext(LOGGER::info)
		    .as(StepVerifier::create)
		    .assertNext(n -> assertThat(n).endsWith(" the end"))
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));
	}

	private ExecutorService startProducer(LinkedList<MonoSink<String>> listeners) {
		ExecutorService producer = Executors.newSingleThreadExecutor();
		afterTest.autoDispose(producer::shutdownNow);

		producer.submit(() -> {
			int i = 0;
			while (true) {
				MonoSink<String> sink = listeners.poll();
				if (sink != null) {
					sink.success(Integer.toString(i++));
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					LOGGER.info("Producer stopping");
					return;
				}
			}
		});

		return producer;
	}

	@Test
	public void defaultBoundedElasticConfigurationIsConsistentWithJavadoc() {
		Schedulers.CachedScheduler cachedBoundedElastic = (Schedulers.CachedScheduler) Schedulers.boundedElastic();
		BoundedElasticScheduler boundedElastic = (BoundedElasticScheduler) cachedBoundedElastic.cached;

		//10 x number of CPUs
		assertThat(boundedElastic.maxThreads)
				.as("default boundedElastic size")
				.isEqualTo(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE)
				.isEqualTo(Runtime.getRuntime().availableProcessors() * 10);

		//60s TTL
		assertThat(boundedElastic.ttlMillis)
				.as("default TTL")
				.isEqualTo(BoundedElasticScheduler.DEFAULT_TTL_SECONDS * 1000)
				.isEqualTo(60_000);

		//100K bounded task queueing
		assertThat(boundedElastic.maxTaskQueuedPerThread)
				.as("default task queueing capacity per thread")
				.isEqualTo(100_000)
				.isEqualTo(Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE);

		assertThat(boundedElastic.estimateRemainingTaskCapacity())
				.isEqualTo(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE * Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE);
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "scanName", 3));
		Scheduler withBasicFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, Thread::new, 3));
		Scheduler withTaskCap = afterTest.autoDispose(Schedulers.newBoundedElastic(1, 123, Thread::new, 3));
		Scheduler cached = Schedulers.boundedElastic();

		assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
				.as("withNamedFactory")
				.isEqualTo("boundedElastic(\"scanName\",maxThreads=1,maxTaskQueuedPerThread=unbounded,ttl=3s)");

		assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
				.as("withBasicFactory")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueuedPerThread=unbounded,ttl=3s)");

		assertThat(Scannable.from(withTaskCap).scan(Scannable.Attr.NAME))
				.as("withTaskCap")
				.isEqualTo("boundedElastic(maxThreads=1,maxTaskQueuedPerThread=123,ttl=3s)");

		assertThat(cached)
				.as("boundedElastic() is cached")
				.is(SchedulersTest.CACHED_SCHEDULER);
		assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
				.as("default boundedElastic()")
				.isEqualTo("Schedulers.boundedElastic()");
	}

	@Test
	public void scanWorkerName() {
		Scheduler withNamedFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "scanName", 3));
		Scheduler withBasicFactory = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, Thread::new, 3));

		Scheduler.Worker workerWithNamedFactory = afterTest.autoDispose(withNamedFactory.createWorker());
		Scheduler.Worker workerWithBasicFactory = afterTest.autoDispose(withBasicFactory.createWorker());

		assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
				.as("workerWithNamedFactory")
				.isEqualTo("ExecutorServiceWorker");

		assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
				.as("workerWithBasicFactory")
				.isEqualTo("ExecutorServiceWorker");
	}

	@Test
	public void scanCapacityBounded() {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(1, 123, Thread::new, 2));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker deferredWorker = afterTest.autoDispose(scheduler.createWorker());

		assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler capacity").isEqualTo(1);
		assertThat(Scannable.from(activeWorker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(123);
	}

	@Test
	public void scanCapacityUnbounded() {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, Thread::new, 2));
		Scheduler.Worker activeWorker = afterTest.autoDispose(scheduler.createWorker());
		Scheduler.Worker deferredWorker = afterTest.autoDispose(scheduler.createWorker());

		assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler capacity").isEqualTo(1);
		assertThat(Scannable.from(activeWorker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void toStringOfTtlInSplitSeconds() {
		String toString = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, null, 1659, Clock.systemDefaultZone())).toString();
		assertThat(toString).endsWith("ttl=1s)");
	}

	@Test
	public void toStringOfTtlUnderOneSecond() {
		String toString = afterTest.autoDispose(new BoundedElasticScheduler(1, 1, null, 523, Clock.systemDefaultZone())).toString();
		assertThat(toString).endsWith("ttl=523ms)");
	}

	@Test
	public void toStringOfExecutorReflectsBoundedVsUnboundedAndCompletedVsQueued()
			throws InterruptedException {
		BoundedScheduledExecutorService bounded = new BoundedScheduledExecutorService(123, Thread::new);
		BoundedScheduledExecutorService unbounded = new BoundedScheduledExecutorService(Integer.MAX_VALUE, Thread::new);

		try {
			bounded.submit(() -> {});
			unbounded.submit(() -> {});
			bounded.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
			unbounded.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);

			Thread.sleep(10); //give a small window for the task to be picked from the queue and completed

			assertThat(bounded).hasToString("BoundedScheduledExecutorService{IDLE, queued=1/123, completed=1}");
			assertThat(unbounded).hasToString("BoundedScheduledExecutorService{IDLE, queued=1/unbounded, completed=1}");
		}
		finally {
			bounded.shutdownNow();
			unbounded.shutdownNow();
		}
	}

	@Test
	public void toStringOfExecutorReflectsIdleVsActive() throws InterruptedException {
		BoundedScheduledExecutorService bounded = new BoundedScheduledExecutorService(123, Thread::new);
		BoundedScheduledExecutorService unbounded = new BoundedScheduledExecutorService(Integer.MAX_VALUE, Thread::new);

		try {
			bounded.submit(ThrowingRunnable.unchecked(() -> Thread.sleep(1000)));
			unbounded.submit(ThrowingRunnable.unchecked(() -> Thread.sleep(1000)));

			Thread.sleep(10); //give a small window for the task to be picked from the queue to reflect active

			assertThat(bounded).hasToString("BoundedScheduledExecutorService{ACTIVE, queued=0/123, completed=0}");
			assertThat(unbounded).hasToString("BoundedScheduledExecutorService{ACTIVE, queued=0/unbounded, completed=0}");
		}
		finally {
			bounded.shutdownNow();
			unbounded.shutdownNow();
		}
	}
}

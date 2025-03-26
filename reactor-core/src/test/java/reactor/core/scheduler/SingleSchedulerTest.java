/*
 * Copyright (c) 2015-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;
import static reactor.core.scheduler.SingleScheduler.TERMINATED;

/**
 * @author Stephane Maldini
 */
public class SingleSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.newSingle("SingleSchedulerTest");
	}

	@Override
	protected Scheduler freshScheduler() {
		return Schedulers.factory.newSingle(new ReactorThreadFactory(
				"SingleSchedulerTest", SingleScheduler.COUNTER, false, true,
				Schedulers::defaultUncaughtException
		));
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Override
	protected boolean shouldCheckMultipleDisposeGracefully() {
		return true;
	}

	@Override
	protected boolean isTerminated(Scheduler s) {
		SingleScheduler scheduler = (SingleScheduler) s;
		return scheduler.state.currentResource.isTerminated();
	}

	@Test
	public void smokeTestDelay() {
		for (int i = 0; i < 20; i++) {
			Scheduler s = Schedulers.newSingle("test");
			AtomicLong start = new AtomicLong();
			AtomicLong end = new AtomicLong();

			try {
				StepVerifier.create(Mono
						.delay(Duration.ofMillis(100), s)
						.log()
						.doOnSubscribe(sub -> start.set(System.nanoTime()))
						.doOnTerminate(() -> end.set(System.nanoTime()))
				)
				            .expectSubscription()
				            .expectNext(0L)
				            .verifyComplete();

				long endValue = end.longValue();
				long startValue = start.longValue();
				long measuredDelay = endValue - startValue;
				long measuredDelayMs = TimeUnit.NANOSECONDS.toMillis(measuredDelay);
				assertThat(measuredDelayMs)
						.as("iteration %s, measured delay %s nanos, start at %s nanos, end at %s nanos", i, measuredDelay, startValue, endValue)
						.isGreaterThanOrEqualTo(100L)
						.isLessThan(200L);
			}
			finally {
				s.dispose();
			}
		}
	}

	@Test
	public void smokeTestInterval() {
		Scheduler s = Schedulers.newSingle("test");

		try {
			StepVerifier.create(Flux.interval(Duration.ofMillis(100), Duration.ofMillis(200), s))
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(100))
			            .expectNext(0L)
			            .expectNoEvent(Duration.ofMillis(200))
			            .expectNext(1L)
			            .expectNoEvent(Duration.ofMillis(200))
			            .expectNext(2L)
			            .thenCancel();
		}
		finally {
			s.dispose();
		}
	}

	@Test
	public void lotsOfTasks() throws Exception {
	    System.gc();
	    Thread.sleep(200);
	    long before = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
	    Scheduler s = Schedulers.newSingle("scheduler");
	    try {
	        Worker w = s.createWorker();
	        try {
	            CountDownLatch cdl = new CountDownLatch(1_000_000);
	            Runnable r = cdl::countDown;
    	        for (int i = 0; i < 1_000_000; i++) {
    	            w.schedule(r);
    	        }
    	        
    	        assertThat(cdl.await(5, TimeUnit.SECONDS)).isTrue();

    	        System.gc();
    	        Thread.sleep(200);

                long after = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

    	        assertThat(before + 20_000_000).as("%,d -> %,d", before, after).isGreaterThan(after);
	        } finally {
	            w.dispose();
	        }
	    } finally {
	        s.dispose();
	    }
	}


	@Test
	void independentWorkers() throws InterruptedException {
		Scheduler timer = afterTest.autoDispose(Schedulers.newSingle("test-timer"));

		Worker w1 = timer.createWorker();

		Worker w2 = timer.createWorker();

		CountDownLatch cdl = new CountDownLatch(1);

		w1.dispose();

		assertThatExceptionOfType(Throwable.class).isThrownBy(() -> {
			w1.schedule(() -> { });
		});

		w2.schedule(cdl::countDown);

		if (!cdl.await(1, TimeUnit.SECONDS)) {
			fail("Worker 2 didn't execute in time");
		}
		w2.dispose();
	}

	@Test
	void massCancel() {
		Scheduler timer = afterTest.autoDispose(Schedulers.newSingle("test-timer"));
		Worker w1 = timer.createWorker();

		AtomicInteger counter = new AtomicInteger();

		Runnable task = counter::getAndIncrement;

		int tasks = 10;

		Disposable[] c = new Disposable[tasks];

		for (int i = 0; i < tasks; i++) {
			c[i] = w1.schedulePeriodically(task, 500, 500, TimeUnit.MILLISECONDS);
		}

		w1.dispose();

		for (int i = 0; i < tasks; i++) {
			assertThat(c[i].isDisposed()).isTrue();
		}

		assertThat(counter).hasValue(0);
	}


	@Test
	public void scanName() {
		Scheduler withNamedFactory = Schedulers.newSingle("scanName");
		Scheduler withBasicFactory = Schedulers.newSingle(Thread::new);
		Scheduler cached = Schedulers.single();

		Scheduler.Worker workerWithNamedFactory = withNamedFactory.createWorker();
		Scheduler.Worker workerWithBasicFactory = withBasicFactory.createWorker();

		try {
			assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
					.as("withNamedFactory")
					.isEqualTo("single(\"scanName\")");

			assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
					.as("withBasicFactory")
					.isEqualTo("single()");

			assertThat(cached)
					.as("single() is cached")
					.is(SchedulersTest.CACHED_SCHEDULER);
			assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
					.as("default single()")
					.isEqualTo("Schedulers.single()");

			assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
					.as("workerWithNamedFactory")
					.isEqualTo("ExecutorServiceWorker");

			assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
					.as("workerWithBasicFactory")
					.isEqualTo("ExecutorServiceWorker");
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
		Scheduler scheduler = Schedulers.newSingle(Thread::new);

		try {
			assertThat(scheduler)
					.matches(s -> Scannable.from(s).isScanAvailable(), "isScanAvailable")
					.satisfies(s -> assertThat(Scannable.from(s).scan(Scannable.Attr.CAPACITY)).isEqualTo(1));
		}
		finally {
			scheduler.dispose();
		}
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	@SuppressWarnings("deprecation")
	void restartSupported(boolean disposeGracefully) {
		Scheduler s = Schedulers.newSingle("restartSupported");
		if (disposeGracefully) {
			s.disposeGracefully().timeout(Duration.ofSeconds(1)).subscribe();
		} else {
			s.dispose();
		}
		SchedulerState<ScheduledExecutorService> stateBefore = ((SingleScheduler) s).state;
		assertThat(stateBefore.currentResource).as("SHUTDOWN").isSameAs(TERMINATED);

		// TODO: in 3.6.x: remove restart capability and this validation
		s.start();

		assertThat(((SingleScheduler) s).state.currentResource)
				.isNotSameAs(stateBefore.currentResource)
				.isInstanceOfSatisfying(ScheduledExecutorService.class,
						executor -> {
							assertThat(executor.isShutdown()).isFalse();
							assertThat(executor.isTerminated()).isFalse();
						});
	}
}

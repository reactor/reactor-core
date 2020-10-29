/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.*;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public class SingleSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.newSingle("SingleSchedulerTest");
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
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
}

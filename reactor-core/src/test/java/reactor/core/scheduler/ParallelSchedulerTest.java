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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import com.pivovarit.function.ThrowingRunnable;

import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class ParallelSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.newParallel("ParallelSchedulerTest");
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Test
	public void startAndDecorationImplicit() {
		AtomicInteger decorationCount = new AtomicInteger();
		Schedulers.setExecutorServiceDecorator("startAndDecorationImplicit", (s, srv) -> {
			decorationCount.incrementAndGet();
			return srv;
		});

		final int parallelismAndExpectedImplicitStart = 4;

		final Scheduler scheduler = afterTest.autoDispose(new ParallelScheduler(parallelismAndExpectedImplicitStart, Thread::new));
		afterTest.autoDispose(() -> Schedulers.removeExecutorServiceDecorator("startAndDecorationImplicit"));

		assertThat(decorationCount).as("before schedule").hasValue(0);
		//first scheduled task implicitly starts the scheduler and thus creates _parallelism_ workers/executorServices
		scheduler.schedule(ThrowingRunnable.unchecked(() -> Thread.sleep(100)));
		assertThat(decorationCount).as("after schedule").hasValue(parallelismAndExpectedImplicitStart);
		//second scheduled task runs on a started scheduler and doesn't create further executors
		scheduler.schedule(() -> {});
		assertThat(decorationCount).as("after 2nd schedule").hasValue(parallelismAndExpectedImplicitStart);
	}

	@Test
	public void negativeParallelism() throws Exception {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Schedulers.newParallel("test", -1);
		});
	}

	@Test
	public void scheduledDoesntReject() {
		Scheduler s = scheduler();

		assertThat(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isNotNull();
		assertThat(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isNotNull();

		Scheduler.Worker w = s.createWorker();
		assertThat(w.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("worker delayed scheduling")
				.isNotNull();
		assertThat(w.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("worker periodic scheduling")
				.isNotNull();
	}

	@Test
	public void smokeTestDelay() {
		for (int i = 0; i < 20; i++) {
			Scheduler s = Schedulers.newParallel("test");
			AtomicLong start = new AtomicLong();
			AtomicLong end = new AtomicLong();

			try {
				StepVerifier.create(Mono
						.delay(Duration.ofMillis(100), s)
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
		Scheduler s = scheduler();

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
	public void shouldPickEvenly() throws Exception {
		int n = 4;
		int m = 25;

		Scheduler scheduler = Schedulers.newParallel("test", n);
		CountDownLatch latch = new CountDownLatch(m*n);
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

		for (int i = 0; i < m * n; i++) {
			scheduler.schedule(() -> {
				String threadName = Thread.currentThread().getName();
				map.compute(threadName, (name, val) -> Optional.ofNullable(val).map(x -> x+1).orElse(1));
				latch.countDown();
			});
		}

		latch.await();
		assertThat(map.values()).containsOnly(m);
	}

	@Test
	public void scanName() {
		Scheduler withNamedFactory = Schedulers.newParallel("scanName", 12);
		Scheduler withBasicFactory = Schedulers.newParallel(12, Thread::new);
		Scheduler cached = Schedulers.parallel();

		Scheduler.Worker workerWithNamedFactory = withNamedFactory.createWorker();
		Scheduler.Worker workerWithBasicFactory = withBasicFactory.createWorker();

		try {
			assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
					.as("withNamedFactory")
					.isEqualTo("parallel(12,\"scanName\")");

			assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
					.as("withBasicFactory")
					.isEqualTo("parallel(12)");

			assertThat(cached)
					.as("parallel() is cached")
					.is(SchedulersTest.CACHED_SCHEDULER);
			assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
					.as("default parallel()")
					.isEqualTo("Schedulers.parallel()");

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
		Scheduler scheduler = Schedulers.newParallel(12, Thread::new);

		try {
			assertThat(scheduler)
					.matches(s -> Scannable.from(s).isScanAvailable(), "isScanAvailable")
					.satisfies(s -> assertThat(Scannable.from(s).scan(Scannable.Attr.CAPACITY)).isEqualTo(12));
		}
		finally {
			scheduler.dispose();
		}
	}
}

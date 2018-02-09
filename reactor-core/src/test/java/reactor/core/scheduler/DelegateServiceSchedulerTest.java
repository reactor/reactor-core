/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class DelegateServiceSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.fromExecutor(Executors.newSingleThreadScheduledExecutor());
	}

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Test
	public void notScheduledRejects() {
		Scheduler s = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor());
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());

		Worker w = s.createWorker();
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> w.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("worker delayed scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> w.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("worker periodic scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
	}

	@Test
	public void scheduledDoesntReject() {
		Scheduler s = Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor());
		assertThat(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isNotNull();
		assertThat(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isNotNull();

		Worker w = s.createWorker();
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
			Scheduler s = Schedulers.fromExecutorService(Executors.newScheduledThreadPool(1));
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
	public void scanName() {
		Scheduler fixedThreadPool = Schedulers.fromExecutorService(Executors.newFixedThreadPool(3));
		Scheduler cachedThreadPool = Schedulers.fromExecutorService(Executors.newCachedThreadPool());
		Scheduler singleThread = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor());

		try {
			assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.NAME))
					.as("fixedThreadPool")
					.startsWith("fromExecutorService(java.util.concurrent.ThreadPoolExecutor@")
					.endsWith("[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0])");
			assertThat(Scannable.from(cachedThreadPool).scan(Scannable.Attr.NAME))
					.as("cachedThreadPool")
					.startsWith("fromExecutorService(java.util.concurrent.ThreadPoolExecutor@")
					.endsWith("[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0])");
			assertThat(Scannable.from(singleThread).scan(Scannable.Attr.NAME))
					.as("singleThread")
					.startsWith("fromExecutorService(java.util.concurrent.Executors$FinalizableDelegatedExecutorService@")
					.endsWith(")");
		}
		finally {
			fixedThreadPool.dispose();
			cachedThreadPool.dispose();
			singleThread.dispose();
		}
	}

	@Test
	public void scanExecutorAttributes() {
		Scheduler fixedThreadPool = Schedulers.fromExecutorService(Executors.newFixedThreadPool(3));

		Long test = Integer.MAX_VALUE + 1L;
		System.out.println(test.intValue() == Integer.MAX_VALUE);

		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.CAPACITY)).isEqualTo(3);
		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.BUFFERED)).isZero();
		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.LARGE_BUFFERED)).isZero();

		fixedThreadPool.schedule(() -> {
			assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.BUFFERED)).isNotZero();
			assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.LARGE_BUFFERED)).isNotZero();
		});
	}
}

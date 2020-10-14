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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import com.pivovarit.function.ThrowingRunnable;

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

	@Override
	protected boolean shouldCheckSupportRestart() {
		return false;
	}

	@Test
	public void startAndDecorationImplicit() {
		AtomicInteger decorationCount = new AtomicInteger();
		Schedulers.setExecutorServiceDecorator("startAndDecorationImplicit", (s, srv) -> {
			decorationCount.incrementAndGet();
			return srv;
		});
		final Scheduler scheduler = afterTest.autoDispose(new DelegateServiceScheduler("startAndDecorationImplicitExecutorService", Executors.newSingleThreadExecutor()));
		afterTest.autoDispose(() -> Schedulers.removeExecutorServiceDecorator("startAndDecorationImplicit"));

		assertThat(decorationCount).as("before schedule").hasValue(0);
		//first scheduled task implicitly starts the scheduler and thus creates the executor service
		scheduler.schedule(ThrowingRunnable.unchecked(() -> Thread.sleep(100)));
		assertThat(decorationCount).as("after schedule").hasValue(1);
		//second scheduled task runs on a started scheduler and doesn't create further executors
		scheduler.schedule(() -> {});
		assertThat(decorationCount).as("after 2nd schedule").hasValue(1);
	}

	@Test
	public void notScheduledRejects() {
		Scheduler s = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newSingleThreadExecutor()));
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable());

		Worker w = afterTest.autoDispose(s.createWorker());
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
		Scheduler s = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor()));
		assertThat(s.schedule(() -> {}, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct delayed scheduling")
				.isNotNull();
		assertThat(s.schedulePeriodically(() -> {}, 100, 100, TimeUnit.MILLISECONDS))
				.describedAs("direct periodic scheduling")
				.isNotNull();

		Worker w = afterTest.autoDispose(s.createWorker());
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
			Scheduler s = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newScheduledThreadPool(1)));
			AtomicLong start = new AtomicLong();
			AtomicLong end = new AtomicLong();

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
	}

	@Test
	public void smokeTestInterval() {
		Scheduler s = afterTest.autoDispose(scheduler());

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

	@Test
	public void scanNameAnonymous() {
		final ExecutorService fixedExecutor = Executors.newFixedThreadPool(3);
		final ExecutorService cachedExecutor = Executors.newCachedThreadPool();
		final ExecutorService singleExecutor = Executors.newSingleThreadExecutor();

		Scheduler fixedThreadPool = afterTest.autoDispose(Schedulers.fromExecutorService(fixedExecutor));
		Scheduler cachedThreadPool = afterTest.autoDispose(Schedulers.fromExecutorService(cachedExecutor));
		Scheduler singleThread = afterTest.autoDispose(Schedulers.fromExecutorService(singleExecutor));

		String fixedId = Integer.toHexString(System.identityHashCode(fixedExecutor));
		String cachedId = Integer.toHexString(System.identityHashCode(cachedExecutor));
		String singleId = Integer.toHexString(System.identityHashCode(singleExecutor));

		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.NAME))
				.as("fixedThreadPool")
				.isEqualTo("fromExecutorService(anonymousExecutor@" + fixedId + ")");
		assertThat(Scannable.from(cachedThreadPool).scan(Scannable.Attr.NAME))
				.as("cachedThreadPool")
				.isEqualTo("fromExecutorService(anonymousExecutor@" + cachedId + ")");
		assertThat(Scannable.from(singleThread).scan(Scannable.Attr.NAME))
				.as("singleThread")
				.isEqualTo("fromExecutorService(anonymousExecutor@" + singleId + ")");
	}

	@Test
	public void scanNameExplicit() {
		Scheduler fixedThreadPool = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newFixedThreadPool(3), "fixedThreadPool(3)"));
		Scheduler cachedThreadPool = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newCachedThreadPool(), "cachedThreadPool"));
		Scheduler singleThread = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(), "singleThreadExecutor"));

		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.NAME))
				.as("fixedThreadPool")
				.isEqualTo("fromExecutorService(fixedThreadPool(3))");
		assertThat(Scannable.from(cachedThreadPool).scan(Scannable.Attr.NAME))
				.as("cachedThreadPool")
				.isEqualTo("fromExecutorService(cachedThreadPool)");
		assertThat(Scannable.from(singleThread).scan(Scannable.Attr.NAME))
				.as("singleThread")
				.isEqualTo("fromExecutorService(singleThreadExecutor)");
	}

	@Test
	public void scanExecutorAttributes() {
		Scheduler fixedThreadPool = afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newFixedThreadPool(3)));

		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.CAPACITY)).isEqualTo(3);
		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.BUFFERED)).isZero();
		assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.LARGE_BUFFERED)).isZero();

		fixedThreadPool.schedule(() -> {
			assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.BUFFERED)).isNotZero();
			assertThat(Scannable.from(fixedThreadPool).scan(Scannable.Attr.LARGE_BUFFERED)).isNotZero();
		});
	}
}

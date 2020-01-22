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

package reactor.test.scheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class VirtualTimeSchedulerTests {

	@Test
	public void cancelledAndEmptyConstantsAreNotSame() {
		assertThat(VirtualTimeScheduler.CANCELLED).isNotSameAs(VirtualTimeScheduler.EMPTY);

		assertThat(VirtualTimeScheduler.CANCELLED.isDisposed()).isTrue();
		assertThat(VirtualTimeScheduler.EMPTY.isDisposed()).isFalse();
	}

	@Test
	public void allEnabled() {
		assertThat(Schedulers.newParallel("")).isNotInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newElastic("")).isNotInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "")).isNotInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newSingle("")).isNotInstanceOf(VirtualTimeScheduler.class);

		VirtualTimeScheduler.getOrSet();

		assertThat(Schedulers.newParallel("")).isInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newElastic("")).isInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "")).isInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newSingle("")).isInstanceOf(VirtualTimeScheduler.class);

		VirtualTimeScheduler t = VirtualTimeScheduler.get();

		Assert.assertSame(Schedulers.newParallel(""), t);
		Assert.assertSame(Schedulers.newElastic(""), t);
		Assert.assertSame(Schedulers.newBoundedElastic(5, Integer.MAX_VALUE, ""), t); //same even though different parameter
		Assert.assertSame(Schedulers.newSingle(""), t);
	}

	@Test
	public void enableProvidedAllSchedulerIdempotent() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		VirtualTimeScheduler.getOrSet(vts);

		Assert.assertSame(vts, uncache(Schedulers.single()));
		Assert.assertFalse(vts.shutdown);


		VirtualTimeScheduler.getOrSet(vts);

		Assert.assertSame(vts, uncache(Schedulers.single()));
		Assert.assertFalse(vts.shutdown);
	}

	@Test
	public void enableTwoSimilarSchedulersUsesFirst() {
		VirtualTimeScheduler vts1 = VirtualTimeScheduler.create();
		VirtualTimeScheduler vts2 = VirtualTimeScheduler.create();

		VirtualTimeScheduler firstEnableResult = VirtualTimeScheduler.getOrSet(vts1);
		VirtualTimeScheduler secondEnableResult = VirtualTimeScheduler.getOrSet(vts2);

		Assert.assertSame(vts1, firstEnableResult);
		Assert.assertSame(vts1, secondEnableResult);
		Assert.assertSame(vts1, uncache(Schedulers.single()));
		Assert.assertFalse(vts1.shutdown);
	}

	@Test
	public void disposedSchedulerIsStillCleanedUp() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.dispose();
		assertThat(VirtualTimeScheduler.isFactoryEnabled()).isFalse();

		StepVerifier.withVirtualTime(() -> Mono.just("foo"),
				() -> vts, Long.MAX_VALUE)
	                .then(() -> assertThat(VirtualTimeScheduler.isFactoryEnabled()).isTrue())
	                .then(() -> assertThat(VirtualTimeScheduler.get()).isSameAs(vts))
	                .expectNext("foo")
	                .verifyComplete();

		assertThat(VirtualTimeScheduler.isFactoryEnabled()).isFalse();

		StepVerifier.withVirtualTime(() -> Mono.just("foo"))
	                .then(() -> assertThat(VirtualTimeScheduler.isFactoryEnabled()).isTrue())
	                .then(() -> assertThat(VirtualTimeScheduler.get()).isNotSameAs(vts))
	                .expectNext("foo")
	                .verifyComplete();

		assertThat(VirtualTimeScheduler.isFactoryEnabled()).isFalse();
	}


	@Test
	public void captureNowInScheduledTask() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create(true);
		List<Long> singleExecutionsTimestamps = new ArrayList<>();
		List<Long> periodicExecutionTimestamps = new ArrayList<>();

		try {
			vts.advanceTimeBy(Duration.ofMillis(100));

			vts.schedule(() -> singleExecutionsTimestamps.add(vts.now(TimeUnit.MILLISECONDS)),
					100, TimeUnit.MILLISECONDS);

			vts.schedule(() -> singleExecutionsTimestamps.add(vts.now(TimeUnit.MILLISECONDS)),
					456, TimeUnit.MILLISECONDS);

			vts.schedulePeriodically(() -> periodicExecutionTimestamps.add(vts.now(TimeUnit.MILLISECONDS)),
					0, 100, TimeUnit.MILLISECONDS);

			vts.advanceTimeBy(Duration.ofMillis(1000));

			assertThat(singleExecutionsTimestamps)
					.as("single executions")
					.containsExactly(100L, 456L + 100L);
			assertThat(periodicExecutionTimestamps)
					.as("periodic executions")
					.containsExactly(100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L, 1100L);
		}
		finally {
			vts.dispose();
		}
	}

	@Test
	public void nestedSchedule() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		List<Long> singleExecutionsTimestamps = new ArrayList<>();

		try {
			vts.schedule(() -> vts.schedule(
					() -> singleExecutionsTimestamps.add(vts.now(TimeUnit.MILLISECONDS)),
					100, TimeUnit.MILLISECONDS
					),
					300, TimeUnit.MILLISECONDS);

			vts.advanceTimeBy(Duration.ofMillis(1000));

			assertThat(singleExecutionsTimestamps)
					.as("single executions")
					.containsExactly(400L);
		}
		finally {
			vts.dispose();
		}
	}

	@Test
	public void racingAdvanceTimeOnEmptyQueue() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		try {
			for (int i = 1; i <= 100; i++) {
				RaceTestUtils.race(
						() -> vts.advanceTimeBy(Duration.ofSeconds(10)),
						() -> vts.advanceTimeBy(Duration.ofSeconds(3)));

				assertThat(vts.now(TimeUnit.MILLISECONDS))
						.as("iteration " + i)
						.isEqualTo(13_000 * i);

				assertThat(vts.nanoTime)
						.as("now() == nanoTime in iteration " + i)
						.isEqualTo(vts.now(TimeUnit.NANOSECONDS));
			}
		}
		finally {
			vts.dispose();
		}
	}

	@Test
	public void racingAdvanceTimeOnFullQueue() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		try {
			vts.schedule(() -> {}, 10, TimeUnit.HOURS);
			for (int i = 1; i <= 100; i++) {
				reactor.test.util.RaceTestUtils.race(
						() -> vts.advanceTimeBy(Duration.ofSeconds(10)),
						() -> vts.advanceTimeBy(Duration.ofSeconds(3)));

				assertThat(vts.now(TimeUnit.MILLISECONDS))
						.as("now() iteration " + i)
						.isEqualTo(13_000 * i);

				assertThat(vts.nanoTime)
						.as("now() == nanoTime in iteration " + i)
						.isEqualTo(vts.now(TimeUnit.NANOSECONDS));
			}
		}
		finally {
			vts.dispose();
		}
	}

	@Test
	public void racingAdvanceTimeOnVaryingQueue() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create(true);
		AtomicInteger count = new AtomicInteger();
		try {
			for (int i = 1; i <= 100; i++) {
				RaceTestUtils.race(
						() -> vts.advanceTimeBy(Duration.ofSeconds(10)),
						() -> vts.advanceTimeBy(Duration.ofSeconds(3)));

				if (i % 10 == 0) {
					vts.schedule(count::incrementAndGet, 14, TimeUnit.SECONDS);
				}

				assertThat(vts.now(TimeUnit.MILLISECONDS))
						.as("now() iteration " + i)
						.isEqualTo(13_000 * i);
			}
			assertThat(count).as("scheduled task run").hasValue(10);

			assertThat(vts.nanoTime)
					.as("now() == nanoTime")
					.isEqualTo(vts.now(TimeUnit.NANOSECONDS));

			assertThat(vts.deferredNanoTime).as("cleared deferredNanoTime").isZero();
		}
		finally {
			vts.dispose();
		}
	}

	@Test
	public void scheduledTaskCount() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		assertThat(vts.getScheduledTaskCount()).as("initial value").isEqualTo(0);

		vts.schedule(() -> {
		});
		assertThat(vts.getScheduledTaskCount()).as("a task scheduled").isEqualTo(1);
	}

	@Test
	public void scheduledTaskCountWithInitialDelay() {
		// schedule with delay
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.schedule(() -> {
		}, 10, TimeUnit.DAYS);
		assertThat(vts.getScheduledTaskCount()).as("scheduled in future").isEqualTo(1);

		vts.advanceTimeBy(Duration.ofDays(11));
		assertThat(vts.getScheduledTaskCount()).as("time advanced").isEqualTo(1);
	}

	@Test
	public void scheduledTaskCountWithNoInitialDelay() {
		// schedulePeriodically with no initial delay
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.schedulePeriodically(() -> {
		}, 0, 5, TimeUnit.DAYS);

		assertThat(vts.getScheduledTaskCount())
			.as("initial delay task performed and scheduled for the first periodical task")
			.isEqualTo(2);

		vts.advanceTimeBy(Duration.ofDays(5));
		assertThat(vts.getScheduledTaskCount())
			.as("scheduled for the second periodical task")
			.isEqualTo(3);
	}

	@Test
	public void scheduledTaskCountBySchedulePeriodically() {
		// schedulePeriodically with initial delay
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.schedulePeriodically(() -> {
		}, 10, 5, TimeUnit.DAYS);
		assertThat(vts.getScheduledTaskCount())
			.as("scheduled for initial delay task")
			.isEqualTo(1);

		vts.advanceTimeBy(Duration.ofDays(1));
		assertThat(vts.getScheduledTaskCount())
			.as("Still on initial delay")
			.isEqualTo(1);

		vts.advanceTimeBy(Duration.ofDays(10));
		assertThat(vts.getScheduledTaskCount())
			.as("first periodical task scheduled after initial one")
			.isEqualTo(2);

		vts.advanceTimeBy(Duration.ofDays(5));
		assertThat(vts.getScheduledTaskCount())
			.as("second periodical task scheduled")
			.isEqualTo(3);
	}

	@SuppressWarnings("unchecked")
	private static Scheduler uncache(Scheduler potentialCached) {
		if (potentialCached instanceof Supplier) {
			return ((Supplier<Scheduler>) potentialCached).get();
		}
		return potentialCached;
	}

	@After
	public void cleanup() {
		VirtualTimeScheduler.reset();
	}

}

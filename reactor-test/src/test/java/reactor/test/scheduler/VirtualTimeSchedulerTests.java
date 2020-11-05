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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
		@SuppressWarnings("deprecation") // To be removed in 3.5 alongside Schedulers.newElastic
		Scheduler elastic1 = Schedulers.newElastic("");
		assertThat(elastic1).isNotInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "")).isNotInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newSingle("")).isNotInstanceOf(VirtualTimeScheduler.class);

		VirtualTimeScheduler.getOrSet();

		assertThat(Schedulers.newParallel("")).isInstanceOf(VirtualTimeScheduler.class);
		@SuppressWarnings("deprecation") // To be removed in 3.5 alongside Schedulers.newElastic
		Scheduler elastic2 = Schedulers.newElastic("");
		assertThat(elastic2).isInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "")).isInstanceOf(VirtualTimeScheduler.class);
		assertThat(Schedulers.newSingle("")).isInstanceOf(VirtualTimeScheduler.class);

		VirtualTimeScheduler t = VirtualTimeScheduler.get();

		assertThat(Schedulers.newParallel("")).isSameAs(t);
		@SuppressWarnings("deprecation") // To be removed in 3.5 alongside Schedulers.newElastic
		Scheduler elastic3 = Schedulers.newElastic("");
		assertThat(elastic3).isSameAs(t);
		assertThat(Schedulers.newBoundedElastic(5, Integer.MAX_VALUE, "")).isSameAs(t); //same even though different parameter
		assertThat(Schedulers.newSingle("")).isSameAs(t);
	}

	@Test
	public void enableProvidedAllSchedulerIdempotent() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		VirtualTimeScheduler.getOrSet(vts);

		assertThat(vts).isSameAs(uncache(Schedulers.single()));
		assertThat(vts.shutdown).isFalse();


		VirtualTimeScheduler.getOrSet(vts);

		assertThat(vts).isSameAs(uncache(Schedulers.single()));
		assertThat(vts.shutdown).isFalse();
	}

	@Test
	public void enableTwoSimilarSchedulersUsesFirst() {
		VirtualTimeScheduler vts1 = VirtualTimeScheduler.create();
		VirtualTimeScheduler vts2 = VirtualTimeScheduler.create();

		VirtualTimeScheduler firstEnableResult = VirtualTimeScheduler.getOrSet(vts1);
		VirtualTimeScheduler secondEnableResult = VirtualTimeScheduler.getOrSet(vts2);

		assertThat(vts1).isSameAs(firstEnableResult);
		assertThat(vts1).isSameAs(secondEnableResult);
		assertThat(vts1).isSameAs(uncache(Schedulers.single()));
		assertThat(vts1.shutdown).isFalse();
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

	@Test
	public void getOrSetWithDefer() {
		AtomicReference<VirtualTimeScheduler> vts1 = new AtomicReference<>();
		AtomicReference<VirtualTimeScheduler> vts2 = new AtomicReference<>();
		RaceTestUtils.race(
				() -> vts1.set(VirtualTimeScheduler.getOrSet(true)),
				() -> vts2.set(VirtualTimeScheduler.getOrSet(true))
		);

		assertThat(vts1.get().defer).isTrue();
		assertThat(vts2.get()).isSameAs(vts1.get());
	}


	@Test
	public void resetRestoresSnapshotOfSchedulers() {
		AtomicInteger singleCreated = new AtomicInteger();
		Schedulers.Factory customFactory = new Schedulers.Factory() {
			@Override
			public Scheduler newSingle(ThreadFactory threadFactory) {
				singleCreated.incrementAndGet();
				return Schedulers.Factory.super.newSingle(threadFactory);
			}
		};
		Schedulers.setFactory(customFactory);
		Scheduler originalScheduler = Schedulers.single();

		assertThat(singleCreated).as("created custom pre VTS").hasValue(1);

		//replace custom factory with VTS factory
		VirtualTimeScheduler.getOrSet();
		// trigger cache of VTS in CACHED_SINGLE
		Scheduler vtsScheduler = Schedulers.single();

		assertThat(singleCreated).as("after VTS setup").hasValue(1);
		assertThat(vtsScheduler).as("shared scheduler replaced").isNotSameAs(originalScheduler);
		assertThat(originalScheduler.isDisposed()).as("original isDisposed").isFalse();

		//attempt to restore the original schedulers and factory
		VirtualTimeScheduler.reset();
		Scheduler postResetSharedScheduler = Schedulers.single();
		Scheduler postResetNewScheduler = Schedulers.newSingle("ignored");
		postResetNewScheduler.dispose();

		assertThat(singleCreated).as("total custom created").hasValue(2);
		assertThat(postResetSharedScheduler).as("shared restored").isSameAs(originalScheduler);
		assertThat(postResetNewScheduler).as("new from restoredgt").isNotInstanceOf(VirtualTimeScheduler.class);
	}

	@Test
	public void doubleCreationOfVtsCorrectlyResetsOriginalCustomFactory() {
		AtomicInteger singleCreated = new AtomicInteger();
		Schedulers.Factory customFactory = new Schedulers.Factory() {
			@Override
			public Scheduler newSingle(ThreadFactory threadFactory) {
				singleCreated.incrementAndGet();
				return Schedulers.Factory.super.newSingle(threadFactory);
			}
		};
		Schedulers.setFactory(customFactory);
		Scheduler originalScheduler = Schedulers.single();

		assertThat(singleCreated).as("created custom pre VTS").hasValue(1);

		//replace custom factory with VTS factory
		VirtualTimeScheduler.getOrSet();
		// trigger cache of VTS in CACHED_SINGLE
		Scheduler vtsScheduler = Schedulers.single();

		assertThat(singleCreated).as("after 1st VTS setup").hasValue(1);
		assertThat(vtsScheduler).as("shared scheduler 1st replaced").isNotSameAs(originalScheduler);
		assertThat(originalScheduler.isDisposed()).as("original isDisposed").isFalse();

		//force replacing VTS factory by another VTS factory
		VirtualTimeScheduler.set(VirtualTimeScheduler.create());
		// trigger cache of VTS in CACHED_SINGLE
		Scheduler vtsScheduler2 = Schedulers.single();

		assertThat(singleCreated).as("after 2nd VTS setup").hasValue(1);
		assertThat(vtsScheduler2).as("shared scheduler 2nd replaced")
		                         .isNotSameAs(originalScheduler)
		                         .isNotSameAs(vtsScheduler);
		assertThat(originalScheduler.isDisposed()).as("original isDisposed").isFalse();

		//attempt to restore the original schedulers and factory
		VirtualTimeScheduler.reset();
		Scheduler postResetSharedScheduler = Schedulers.single();
		Scheduler postResetNewScheduler = Schedulers.newSingle("ignored");
		postResetNewScheduler.dispose();

		assertThat(singleCreated).as("total custom created").hasValue(2);
		assertThat(postResetSharedScheduler).as("shared restored").isSameAs(originalScheduler);
		assertThat(postResetNewScheduler).as("new from restoredgt").isNotInstanceOf(VirtualTimeScheduler.class);
	}

	@SuppressWarnings("unchecked")
	private static Scheduler uncache(Scheduler potentialCached) {
		if (potentialCached instanceof Supplier) {
			return ((Supplier<Scheduler>) potentialCached).get();
		}
		return potentialCached;
	}

	@AfterEach
	public void cleanup() {
		VirtualTimeScheduler.reset();
	}

}

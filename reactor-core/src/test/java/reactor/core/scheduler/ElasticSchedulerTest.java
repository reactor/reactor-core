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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import com.pivovarit.function.ThrowingRunnable;
import org.assertj.core.data.Offset;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 * @author Simon Baslé
 */
@SuppressWarnings("deprecation") // This is because of #newElastic() calls, to be removed in 3.5. ElasticScheduler class would then also be removed.
public class ElasticSchedulerTest extends AbstractSchedulerTest {

	private static final Logger LOGGER = Loggers.getLogger(ElasticSchedulerTest.class);

	@Override
	protected Scheduler scheduler() {
		return Schedulers.newElastic("ElasticSchedulerTest");
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Override
	protected boolean shouldCheckSupportRestart() {
		return true;
	}

	@Test
	public void bothStartAndRestartDoNotThrow() {
		Scheduler scheduler = afterTest.autoDispose(scheduler());
		assertThatCode(scheduler::start).as("start").doesNotThrowAnyException();

		scheduler.dispose();
		assertThatCode(scheduler::start).as("restart").doesNotThrowAnyException();
	}

	@Test
	public void negativeTime() throws Exception {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Schedulers.newElastic("test", -1);
		});
	}

	@Test
	@Timeout(10)
	public void eviction() throws Exception {
		Scheduler s = Schedulers.newElastic("test-recycle", 2);
		((ElasticScheduler)s).evictor.shutdownNow();

		try{
			for (int i = 0; i < 100; i++) {
				Disposable d = s.schedule(() -> {
					try {
						Thread.sleep(10000);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				});

				d.dispose();
			}

			while(((ElasticScheduler)s).cache.peek() != null){
				((ElasticScheduler)s).eviction();
				Thread.sleep(100);
			}

			assertThat(((ElasticScheduler)s).all).isEmpty();
		}
		finally {
			s.dispose();
			s.dispose();//noop
		}

		assertThat(((ElasticScheduler)s).cache).isEmpty();
		assertThat(s.isDisposed()).isTrue();
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
			Scheduler s = Schedulers.newElastic("test");
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
	public void scanName() {
		Scheduler withNamedFactory = Schedulers.newElastic("scanName", 1);
		Scheduler withBasicFactory = Schedulers.newElastic(1, Thread::new);
		Scheduler cached = Schedulers.elastic();

		Scheduler.Worker workerWithNamedFactory = withNamedFactory.createWorker();
		Scheduler.Worker workerWithBasicFactory = withBasicFactory.createWorker();

		try {
			assertThat(Scannable.from(withNamedFactory).scan(Scannable.Attr.NAME))
					.as("withNamedFactory")
					.isEqualTo("elastic(\"scanName\")");

			assertThat(Scannable.from(withBasicFactory).scan(Scannable.Attr.NAME))
					.as("withBasicFactory")
					.isEqualTo("elastic()");

			assertThat(cached)
					.as("elastic() is cached")
					.is(SchedulersTest.CACHED_SCHEDULER);
			assertThat(Scannable.from(cached).scan(Scannable.Attr.NAME))
					.as("default elastic()")
					.isEqualTo("Schedulers.elastic()");

			assertThat(Scannable.from(workerWithNamedFactory).scan(Scannable.Attr.NAME))
					.as("workerWithNamedFactory")
					.isEqualTo("elastic(\"scanName\").worker");

			assertThat(Scannable.from(workerWithBasicFactory).scan(Scannable.Attr.NAME))
					.as("workerWithBasicFactory")
					.isEqualTo("elastic().worker");
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
		Scheduler scheduler = Schedulers.newElastic(2, Thread::new);
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			assertThat(Scannable.from(scheduler).scan(Scannable.Attr.CAPACITY)).as("scheduler unbounded").isEqualTo(Integer.MAX_VALUE);
			assertThat(Scannable.from(worker).scan(Scannable.Attr.CAPACITY)).as("worker capacity").isEqualTo(1);
		}
		finally {
			worker.dispose();
			scheduler.dispose();
		}
	}

	@Test
	public void lifoEviction() throws InterruptedException {
		Scheduler scheduler = Schedulers.newElastic("dequeueEviction", 1);
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
					activeAtBeginning = Math.max(0, Thread.activeCount() - otherThreads);
					oldActive = activeAtBeginning;
					LOGGER.info("{} threads active in round 1/{}", activeAtBeginning, fastCount);
				}
				else if (i == fastCount - 1) {
					activeAtEnd = Math.max(0, Thread.activeCount() - otherThreads);
					LOGGER.info("{} threads active in round {}/{}", activeAtEnd, i + 1, fastCount);
				}
				else {
					int newActive = Math.max(0, Thread.activeCount() - otherThreads);
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
	public void doesntRecycleWhileRunningAfterDisposed() throws Exception {
		Scheduler s = Schedulers.newElastic("test-recycle");
		((ElasticScheduler)s).evictor.shutdownNow();

		try {
			AtomicBoolean stop = new AtomicBoolean(false);
			CountDownLatch started = new CountDownLatch(1);
			Disposable d = s.schedule(() -> {
				started.countDown();
				// simulate uninterruptible computation
				for (;;) {
					if (stop.get()) {
						break;
					}
				}
			});
			assertThat(started.await(10, TimeUnit.SECONDS)).as("latch timeout").isTrue();
			d.dispose();

			Thread.sleep(100);
			assertThat(((ElasticScheduler)s).cache).isEmpty();

			stop.set(true);

			Thread.sleep(100);
			assertThat(((ElasticScheduler)s).cache.size()).isEqualTo(1);
		}
		finally {
			s.dispose();
		}
	}

	@Test
	public void recycleOnce() throws Exception {
		Scheduler s = Schedulers.newElastic("test-recycle");
		((ElasticScheduler)s).evictor.shutdownNow();

		try {
			Disposable d = s.schedule(() -> {
				try {
					Thread.sleep(10000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});

			// Dispose twice to test that the executor is returned to the pool only once
			d.dispose();
			d.dispose();

			Thread.sleep(100);
			assertThat(((ElasticScheduler)s).cache.size()).isEqualTo(1);
		}
		finally {
			s.dispose();
		}
	}
}

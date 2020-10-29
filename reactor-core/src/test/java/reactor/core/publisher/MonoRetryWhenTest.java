/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoRetryWhenTest {

	@Test
	public void twoRetryNormalSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Mono<Integer> source = Mono
				.fromCallable(i::incrementAndGet)
				.doOnNext(v -> {
					if (v < 4) {
						throw new RuntimeException("test");
					}
					else {
						bool.set(false);
					}
				})
				.retryWhen(Retry.max(3).filter(e -> bool.get()));

		StepVerifier.create(source)
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRetryErrorSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Mono<Integer> source = Mono
				.fromCallable(i::incrementAndGet)
				.doOnNext(v -> {
					if (v < 4) {
						if (v > 2) {
							bool.set(false);
						}
						throw new RuntimeException("test");
					}
				})
				.retryWhen(Retry.max(3).filter(e -> bool.get()));

		StepVerifier.create(source)
		            .verifyErrorMessage("test");
	}

	Mono<String> exponentialRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Mono.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.success("hey");
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(Retry.from(companion -> companion
				.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
				.flatMap(time -> Mono.delay(Duration.ofSeconds(time)))));
	}

	@Test
	public void exponentialRetry() {
		StepVerifier.withVirtualTime(this::exponentialRetryScenario)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void monoRetryRandomBackoff() {
		AtomicInteger errorCount = new AtomicInteger();
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Mono.error(exception)
				    .doOnError(e -> {
				    	errorCount.incrementAndGet();
				    	elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
				    })
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
						    .jitter(0.1)
				    )
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0))
				.isGreaterThanOrEqualTo(100) //min backoff
				.isCloseTo(100, Percentage.withPercentage(10));
		assertThat(elapsedList.get(2) - elapsedList.get(1))
				.isCloseTo(200, Percentage.withPercentage(10));
		assertThat(elapsedList.get(3) - elapsedList.get(2))
				.isCloseTo(400, Percentage.withPercentage(10));
		assertThat(elapsedList.get(4) - elapsedList.get(3))
				.isCloseTo(800, Percentage.withPercentage(10));
	}

	@Test
	public void monoRetryRandomBackoffDefaultJitter() {
		AtomicInteger errorCount = new AtomicInteger();
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Mono.error(exception)
				    .doOnError(e -> {
				    	errorCount.incrementAndGet();
				    	elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
				    })
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
				    )
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0))
				.isGreaterThanOrEqualTo(100) //min backoff
				.isCloseTo(100, Percentage.withPercentage(50));
		assertThat(elapsedList.get(2) - elapsedList.get(1))
				.isCloseTo(200, Percentage.withPercentage(50));
		assertThat(elapsedList.get(3) - elapsedList.get(2))
				.isCloseTo(400, Percentage.withPercentage(50));
		assertThat(elapsedList.get(4) - elapsedList.get(3))
				.isCloseTo(800, Percentage.withPercentage(50));
	}


	@Test
	public void monoRetryRandomBackoffDefaultMaxDuration() {
		AtomicInteger errorCount = new AtomicInteger();
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Mono.error(exception)
				    .doOnError(e -> {
				    	errorCount.incrementAndGet();
				    	elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
				    })
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100)))
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0))
				.isGreaterThanOrEqualTo(100) //min backoff
				.isCloseTo(100, Percentage.withPercentage(50));
		assertThat(elapsedList.get(2) - elapsedList.get(1))
				.isCloseTo(200, Percentage.withPercentage(50));
		assertThat(elapsedList.get(3) - elapsedList.get(2))
				.isCloseTo(400, Percentage.withPercentage(50));
		assertThat(elapsedList.get(4) - elapsedList.get(3))
				.isCloseTo(800, Percentage.withPercentage(50));
	}

	@Test
	public void monoRetryRandomBackoff_maxBackoffShaves() {
		AtomicInteger errorCount = new AtomicInteger();
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Mono.error(exception)
				    .doOnError(e -> {
				    	errorCount.incrementAndGet();
				    	elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
				    })
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100))
				                    .maxBackoff(Duration.ofMillis(220))
				                    .jitter(0.9)
				    )
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0))
				.isGreaterThanOrEqualTo(100) //min backoff
				.isCloseTo(100, Percentage.withPercentage(90));

		assertThat(elapsedList.get(2) - elapsedList.get(1))
				.isCloseTo(200, Percentage.withPercentage(90))
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
		assertThat(elapsedList.get(3) - elapsedList.get(2))
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
		assertThat(elapsedList.get(4) - elapsedList.get(3))
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
	}

	@Test
	public void monoRetryRandomBackoff_minBackoffFloor() {
		for (int i = 0; i < 50; i++) {
			AtomicInteger errorCount = new AtomicInteger();
			Exception exception = new IOException("boom retry loop #" + i);
			List<Long> elapsedList = new ArrayList<>();

			StepVerifier.withVirtualTime(() ->
					Mono.error(exception)
					    .doOnError(e -> {
						    errorCount.incrementAndGet();
						    elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
					    })
					    .retryWhen(Retry.backoff(1, Duration.ofMillis(100))
							    .maxBackoff(Duration.ofMillis(2000))
							    .jitter(0.9)
					    )
			)
			            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
			            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
			                                                    .hasMessage("Retries exhausted: 1/1")
			                                                    .hasCause(exception))
			            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

			assertThat(errorCount).hasValue(2);
			assertThat(elapsedList).hasSize(2);
			assertThat(elapsedList.get(0)).isEqualTo(0L);
			assertThat(elapsedList.get(1) - elapsedList.get(0))
					.isGreaterThanOrEqualTo(100) //min backoff
					.isCloseTo(100, Percentage.withPercentage(90));
		}
	}

	@Test
	public void monoRetryRandomBackoff_noRandom() {
		AtomicInteger errorCount = new AtomicInteger();
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Mono.error(exception)
				    .doOnError(e -> {
					    errorCount.incrementAndGet();
					    elapsedList.add(Schedulers.parallel().now(TimeUnit.MILLISECONDS));
				    })
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
						    .jitter(0d)
				    )
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0)).isEqualTo(100);
		assertThat(elapsedList.get(2) - elapsedList.get(1)).isEqualTo(200);
		assertThat(elapsedList.get(3) - elapsedList.get(2)).isEqualTo(400);
		assertThat(elapsedList.get(4) - elapsedList.get(3)).isEqualTo(800);
	}

	@Test
	public void monoRetryBackoffWithGivenScheduler() {
		VirtualTimeScheduler backoffScheduler = VirtualTimeScheduler.create();

		Exception exception = new IOException("boom retry");
		AtomicInteger errorCount = new AtomicInteger();

		StepVerifier.create(
				Mono.error(exception)
				    .doOnError(t -> errorCount.incrementAndGet())
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(10))
						    .maxBackoff(Duration.ofMillis(100))
						    .jitter(0)
						    .scheduler(backoffScheduler)
				    )
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(400))
		            .then(() -> assertThat(errorCount).as("errorCount before advanceTime").hasValue(1))
		            .then(() -> backoffScheduler.advanceTimeBy(Duration.ofMillis(400)))
		            .then(() -> assertThat(errorCount).as("errorCount after advanceTime").hasValue(5))
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofMillis(400)); //test should only take roughly the expectNoEvent time
	}


	@Test
	public void monoRetryBackoffRetriesOnGivenScheduler() {
		//the monoRetryBackoffWithGivenScheduler above is not suitable to verify the retry scheduler,
		// as VTS is akin to immediate() and doesn't really change the Thread
		Scheduler backoffScheduler = Schedulers.newSingle("backoffScheduler");
		String main = Thread.currentThread().getName();
		final IllegalStateException exception = new IllegalStateException("boom");
		List<String> threadNames = new ArrayList<>(4);
		try {
			StepVerifier.create(Mono.error(exception)
			                        .doOnError(e -> threadNames.add(Thread.currentThread().getName().replaceFirst("-\\d+", "")))
			                        .retryWhen(Retry.backoff(2, Duration.ofMillis(10))
					                        .maxBackoff(Duration.ofMillis(100))
					                        .jitter(0.5d)
					                        .scheduler(backoffScheduler)
			                        )
			)
			            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
			                                                    .hasMessage("Retries exhausted: 2/2")
			                                                    .hasCause(exception))
			            .verify(Duration.ofMillis(200));

			assertThat(threadNames)
					.as("retry runs on backoffScheduler")
					.containsExactly(main, "backoffScheduler", "backoffScheduler");
		}
		finally {
			backoffScheduler.dispose();
		}
	}

	@Test
	public void scanOperator(){
		Mono<String> parent = Mono.just("foo");
		MonoRetryWhen<String> test = new MonoRetryWhen<>(parent, Retry.backoff(5, Duration.ofMinutes(30)));

	    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

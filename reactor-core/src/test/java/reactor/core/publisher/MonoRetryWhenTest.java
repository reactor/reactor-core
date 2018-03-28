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
package reactor.core.publisher;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.assertj.core.api.LongAssert;
import org.assertj.core.data.Percentage;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoRetryWhenTest {


	Mono<String> exponentialRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Mono.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.success("hey");
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                             .flatMap(time -> Mono.delay(Duration.ofSeconds(time))));
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
				    .retryWithBackoff(4, Duration.ofMillis(100), Duration.ofMillis(2000), 0.1)
		)
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception));

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
				    .retryWithBackoff(4, Duration.ofMillis(100), Duration.ofMillis(220), 0.9)
		)
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception));

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
					    .retryWithBackoff(1, Duration.ofMillis(100), Duration.ofMillis(2000), 0.9)
			)
			            .thenAwait(Duration.ofSeconds(2))
			            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
			                                                    .hasMessage("Retries exhausted: 1/1")
			                                                    .hasCause(exception));

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
				    .retryWithBackoff(4, Duration.ofMillis(100), Duration.ofMillis(2000), 0d)
		)
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception));

		assertThat(errorCount).hasValue(5);
		assertThat(elapsedList.get(0)).isEqualTo(0L);
		assertThat(elapsedList.get(1) - elapsedList.get(0)).isEqualTo(100);
		assertThat(elapsedList.get(2) - elapsedList.get(1)).isEqualTo(200);
		assertThat(elapsedList.get(3) - elapsedList.get(2)).isEqualTo(400);
		assertThat(elapsedList.get(4) - elapsedList.get(3)).isEqualTo(800);
	}

}
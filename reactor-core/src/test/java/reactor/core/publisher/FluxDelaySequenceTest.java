/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.assertj.core.data.Offset;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.FluxDelaySequence.DelaySubscriber;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDelaySequenceTest {

	@Test
	public void delayFirstInterval() {
		Flux<Tuple2<Long, Long>> test = Flux.interval(Duration.ofMillis(50))
		                                    .delaySequence(Duration.ofMillis(500))
		                                    .take(33)
		                                    .elapsed();

		StepVerifier.create(test)
		            .recordWith(ArrayList::new)
		            .assertNext(t2 -> assertThat(t2.getT1()).isGreaterThan(500))
		            .thenConsumeWhile(t2 -> t2.getT1() < 70)
		            .consumeRecordedWith(record -> {
			            assertThat(record.stream().mapToLong(Tuple2::getT2))
			                      .startsWith(0L, 1L, 2L)
			                      .endsWith(30L, 31L, 32L)
			                      .isSorted()
			                      .hasSize(33);
		            })
		            .verifyComplete();
	}

	@Test
	public void delayFirstAsymmetricDelays() {
		Flux<Long> asymmetricDelays = Flux.concat(
				Mono.delay(Duration.ofMillis(400)).then(Mono.just(0L)),
				Mono.delay(Duration.ofMillis(800)).then(Mono.just(1L)),
				Mono.delay(Duration.ofMillis(200)).then(Mono.just(2L)),
				Mono.delay(Duration.ofMillis(300)).then(Mono.just(3L))
		);

		Flux<Tuple2<Long, Long>> test = asymmetricDelays
				.delaySequence(Duration.ofMillis(500))
				.take(33)
				.elapsed();

		Offset<Long> OFFSET_80MS = Offset.offset(80L);

		StepVerifier.create(test)
		            //first is delayed (from subscription) by additional 500ms
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isCloseTo(400L + 500L, OFFSET_80MS);
			            assertThat(t2.getT2()).isEqualTo(0L);
		            })
		            //rest follow same delays as in source
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isCloseTo(800L, OFFSET_80MS);
			            assertThat(t2.getT2()).isEqualTo(1L);
		            })
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isCloseTo(200L, OFFSET_80MS);
			            assertThat(t2.getT2()).isEqualTo(2L);
		            })
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isCloseTo(300L, OFFSET_80MS);
			            assertThat(t2.getT2()).isEqualTo(3L);
		            })
		            .verifyComplete();
	}

	@Ignore("delayElements test for local comparison run")
	@Test
	public void delayElements() {
		Flux<Tuple2<Long, Long>> test = Flux.interval(Duration.ofMillis(50))
		                                    .onBackpressureDrop()
		                                    .delayElements(Duration.ofMillis(500))
		                                    .take(33)
		                                    .elapsed()
		                                    .log();

		StepVerifier.create(test)
		            .thenConsumeWhile(t2 -> t2.getT1() >= 500)
		            .verifyComplete();
	}

	@Test
	public void every50msThenErrorDelaysError() {
		Flux<Long> source = Flux.concat(
				Mono.delay(Duration.ofMillis(50)).then(Mono.just(0L)),
				Mono.delay(Duration.ofMillis(50)).then(Mono.just(1L)),
				Mono.delay(Duration.ofMillis(50)).then(Mono.just(2L)),
				Mono.error(new IllegalStateException("boom"))
		);

		Flux<Long> test = source
				.delaySequence(Duration.ofMillis(1000));

		Duration took = StepVerifier.create(test)
		                            .expectSubscription()
		                            .expectNoEvent(Duration.ofMillis(1040))
		                            .expectNext(0L)
		                            .expectNoEvent(Duration.ofMillis(40))
		                            .expectNext(1L)
		                            .expectNoEvent(Duration.ofMillis(40))
		                            .expectNext(2L)
		                            .expectErrorMessage("boom")
		                            .verify();

		assertThat(took.toMillis())
				.as("error delayed once emitted")
				.isGreaterThanOrEqualTo(1150)
				.isLessThan(1300);
	}

	@Test
	public void emptyErrorErrorsImmediately() {
		Flux<Long> source = Flux.error(new IllegalStateException("boom"));

		Flux<Long> test = source
				.delaySequence(Duration.ofMillis(1000));

		Duration took = StepVerifier.create(test)
		                            .expectSubscription()
		                            .expectErrorMessage("boom")
		                            .verify();

		assertThat(took.toMillis())
				.as("errors immediately")
				.isLessThan(50);
	}

	@Test
	public void emptyCompletesImmediately() {
		Flux<Long> source = Flux.empty();

		Flux<Long> test = source
				.delaySequence(Duration.ofMillis(1000));

		Duration took = StepVerifier.create(test)
		            .expectComplete()
		            .verify();

		assertThat(took.toMillis())
				.as("completes immediately")
				.isLessThan(50);
	}

	@Test
	public void longDelayInMillis() {
		Duration longDelay = Duration.ofMinutes(1);
		long expected = longDelay.toMillis();

		DelaySubscriber<String> subscriber = new DelaySubscriber<>(null, longDelay, null);

		assertThat(subscriber.delay).isEqualTo(expected);
		assertThat(subscriber.timeUnit).isSameAs(TimeUnit.MILLISECONDS);
	}

	@Test
	public void smallDelayInNanos() {
		Duration longDelay = Duration.ofMillis(59_999);
		long expected = longDelay.toNanos();

		DelaySubscriber<String> subscriber = new DelaySubscriber<>(null, longDelay, null);

		assertThat(subscriber.delay).isEqualTo(expected);
		assertThat(subscriber.timeUnit).isSameAs(TimeUnit.NANOSECONDS);
	}

	@Test
	public void onNextAfterCompleteDrops() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(
				TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher
				.flux()
				.delaySequence(Duration.ofMillis(500)))
		            .then(testPublisher::complete)
		            .then(() -> testPublisher.next("foo"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped("foo")
		            .hasNotDroppedErrors();
	}

	@Test
	public void onNextAfterErrorDrops() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(
				TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher
				.flux()
				.delaySequence(Duration.ofMillis(500)))
		            .then(() -> testPublisher.error(new IllegalStateException("boom")))
		            .then(() -> testPublisher.next("foo"))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDropped("foo")
		            .hasNotDroppedErrors();
	}

	@Test
	public void onCompleteAfterComplete() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(
				TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher
				.flux()
				.delaySequence(Duration.ofMillis(500)))
		            .then(testPublisher::complete)
		            .then(testPublisher::complete)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();
	}

	@Test
	public void onErrorAfterCompleteDrops() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(
				TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher
				.flux()
				.delaySequence(Duration.ofMillis(500)))
		            .then(testPublisher::complete)
		            .then(() -> testPublisher.error(new IllegalStateException("boom")))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrorWithMessage("boom");
	}

}
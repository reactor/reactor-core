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

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxDelaySequence.DelaySubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDelaySequenceTest {

	@Test
	public void delayFirstInterval() {
		Supplier<Flux<Tuple2<Long, Long>>> test = () -> Flux.interval(Duration.ofMillis(50))
	                                                    .delaySequence(Duration.ofMillis(500))
	                                                    .elapsed()
	                                                        .take(33);

		StepVerifier.withVirtualTime(test)
		            .thenAwait(Duration.ofMillis(500 + 50))
		            .recordWith(ArrayList::new)
		            .assertNext(t2 -> assertThat(t2.getT1()).isEqualTo(550))
		            .thenAwait(Duration.ofMillis(33 * 50))
		            .thenConsumeWhile(t2 -> t2.getT1() == 50)
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

		Supplier<Flux<Tuple2<Long, Long>>> test = () -> {
			Flux<Long> asymmetricDelays = Flux.concat(
					Mono.delay(Duration.ofMillis(400)).then(Mono.just(0L)),
					Mono.delay(Duration.ofMillis(800)).then(Mono.just(1L)),
					Mono.delay(Duration.ofMillis(200)).then(Mono.just(2L)),
					Mono.delay(Duration.ofMillis(300)).then(Mono.just(3L))
			);

			return asymmetricDelays
					.delaySequence(Duration.ofMillis(500))
					.take(33)
					.elapsed();
		};

		StepVerifier.withVirtualTime(test)
		            //first is delayed (from subscription) by additional 500ms
		            .thenAwait(Duration.ofMillis(500 + 400))
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isEqualTo(400L + 500L);
			            assertThat(t2.getT2()).isEqualTo(0L);
		            })
		            //rest follow same delays as in source
		            .thenAwait(Duration.ofMillis(800))
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isEqualTo(800L);
			            assertThat(t2.getT2()).isEqualTo(1L);
		            })
		            .thenAwait(Duration.ofMillis(200))
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isEqualTo(200L);
			            assertThat(t2.getT2()).isEqualTo(2L);
		            })
		            .thenAwait(Duration.ofMillis(300))
		            .assertNext(t2 -> {
			            assertThat(t2.getT1()).isEqualTo(300L);
			            assertThat(t2.getT2()).isEqualTo(3L);
		            })
		            .verifyComplete();
	}

	@Disabled("delayElements test for local comparison run")
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

		Supplier<Flux<Long>> test = () -> {
			Flux<Long> source = Flux.concat(Mono.delay(Duration.ofMillis(50))
			                                    .then(Mono.just(0L)),
					Mono.delay(Duration.ofMillis(50))
					    .then(Mono.just(1L)),
					Mono.delay(Duration.ofMillis(50))
					    .then(Mono.just(2L)),
					Mono.error(new IllegalStateException("boom")));
			return source.delaySequence(Duration.ofMillis(1000));
		};

		StepVerifier.withVirtualTime(test)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1050))
		            .expectNext(0L)
		            .expectNoEvent(Duration.ofMillis(50))
		            .expectNext(1L)
		            .expectNoEvent(Duration.ofMillis(50))
		            .expectNext(2L)
		            .verifyErrorMessage("boom");
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
	public void allDelayInNanos() {
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

	@Test
	public void scanOperator() {
		FluxDelaySequence<String> test = new FluxDelaySequence<>(Flux.empty(), Duration.ofSeconds(1), Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscriber() {
		Scheduler.Worker worker = Schedulers.immediate().createWorker();

		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		Subscription s = Operators.emptySubscription();

		FluxDelaySequence.DelaySubscriber test = new DelaySubscriber<>(actual, Duration.ofSeconds(1), worker);

		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(s);
		SerializedSubscriber serializedSubscriber = (SerializedSubscriber) test.scan(Scannable.Attr.ACTUAL);
		assertThat(serializedSubscriber)
				.isNotNull()
				.satisfies(ser -> assertThat(ser.actual).isSameAs(actual));

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isEqualTo(worker);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

		test.done = true;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

}

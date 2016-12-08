/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.*;
import static reactor.test.publisher.TestPublisher.Violation.REQUEST_OVERFLOW;

/**
 * @author Arjen Poutsma
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @author Simon Baslé
 */
public class StepVerifierTests {

	@Test
	public void expectNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNext("bar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectInvalidNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
				                              .expectNext("foo")
				                              .expectNext("baz")
				                              .expectComplete()
				                              .verify())
				.withMessageEndingWith("(expected value: baz; actual value: bar)");
	}

	@Test
	public void expectNextAsync() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.parallel());

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNext("bar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNexts() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectInvalidNexts() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
				                              .expectNext("foo", "baz")
				                              .expectComplete()
				                              .verify())
	            .withMessage("expectation \"expectNext(baz)\" failed (expected value: baz; actual value: bar)");
	}

	@Test
	public void expectNextMatches() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNextMatches("foo"::equals)
		            .expectNextMatches("bar"::equals)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectInvalidNextMatches() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNextMatches("foo"::equals)
		            .expectNextMatches("baz"::equals)
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectNextMatches\" failed (predicate failed on value: bar)");
	}

	@Test
	public void consumeNextWith() throws Exception {
		Flux<String> flux = Flux.just("bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
				                              .consumeNextWith(s -> {
					                              if (!"foo".equals(s)) {
						                              throw new AssertionError("e:" + s);
					                              }
				                              })
				                              .expectComplete()
				                              .verify())
				.withMessage("e:bar");
	}

	@Test
	public void consumeNextWith2() throws Exception {
		Flux<String> flux = Flux.just("bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
				                              .consumeNextWith(s -> {
					                              if (!"foo".equals(s)) {
						                              throw new AssertionError(s);
					                              }
				                              })
				                              .expectComplete()
				                              .verify())
				.withMessage("bar");
	}

	@Test
	public void missingNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onNext(bar))");
	}

	@Test
	public void missingNextAsync() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.parallel());

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onNext(bar))");
	}

	@Test
	public void expectNextCount() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNextCountLots() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		StepVerifier.create(flux, 0)
		            .thenRequest(100_000)
		            .expectNextCount(100_000)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .thenRequest(500_000)
		            .expectNextCount(400_000)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNextCountLotsError() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux, 0)
		            .thenRequest(100_000)
		            .expectNextCount(100_000)
		            .thenRequest(900_000)
		            .expectNextCount(900_001)
		            .expectComplete()
		            .verify())
	            .withMessageStartingWith("expectation \"expectNextCount\" failed")
				.withMessageContaining("expected: count = 900001; actual: produced = 900000; signal: onComplete()");
	}

	@Test
	public void expectNextCountLotsUnderRequestErrorReportedAtEnd() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux, 0)
		            .thenRequest(100_000)
		            .expectNextCount(100_000)
		            .thenRequest(500_000)
		            .expectNextCount(499_999)
		            .thenRequest(500_000)
		            .expectNextCount(400_000)
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onNext(999999))");
	}

	@Test
	public void expectNextCount2() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo", "bar")
		            .expectNextCount(2)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNextCount3() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNextCount(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNextCountZero() {
		Flux<String> flux = Flux.empty();

		StepVerifier.create(flux)
		            .expectNextCount(0)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void expectNextCountError() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNextCount(4)
		            .thenCancel()
		            .verify())
				.withMessage("expectation \"expectNextCount\" failed (expected: count = 4; actual: produced = 2; signal: onComplete())");
	}

	@Test
	public void error() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectError()
		            .verify();
	}

	@Test
	public void errorClass() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectError(IllegalArgumentException.class)
		            .verify();
	}

	@Test
	public void errorMessage() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException(
				                        "Error message")));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorMessage("Error message")
		            .verify();
	}

	@Test
	public void errorMatches() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorMatches(t -> t instanceof IllegalArgumentException)
		            .verify();
	}

	@Test
	public void errorMatchesInvalid() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorMatches(t -> t instanceof IllegalStateException)
		            .verify())
	            .withMessage("expectation \"expectErrorMatches\" failed (predicate failed on exception: java.lang.IllegalArgumentException)");
	}

	@Test
	public void consumeErrorWith() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
			            .expectNext("foo")
			            .consumeErrorWith(throwable -> {
				            if (!(throwable instanceof IllegalStateException)) {
					            throw new AssertionError(throwable.getClass()
					                                              .getSimpleName());
				            }
			            })
			            .verify())
	            .withMessage("IllegalArgumentException");
	}

	@Test
	public void request() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux, 1)
		            .thenRequest(1)
		            .expectNext("foo")
		            .thenRequest(1)
		            .expectNext("bar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void cancel() {
		Flux<String> flux = Flux.just("foo", "bar", "baz");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void cancelInvalid() {
		Flux<String> flux = Flux.just("bar", "baz");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .thenCancel()
		            .verify())
	            .withMessage("expectation \"expectNext(foo)\" failed (expected value: foo; actual value: bar)");
	}

	@Test
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier s = StepVerifier.create(flux)
		                             .expectNext("foo")
		                             .expectNext("bar")
		                             .expectComplete();

		s.verify();
		s.verify();
	}

	@Test
	public void verifyThenOnCompleteRange() {
		DirectProcessor<Void> p = DirectProcessor.create();

		Flux<String> flux = Flux.range(0, 3)
		                        .map(d -> "t" + d)
		                        .takeUntilOther(p);

		StepVerifier.create(flux, 2)
		            .expectNext("t0", "t1")
		            .then(p::onComplete)
		            .expectComplete()
		            .verify();

	}

	@Test
	public void verifyDuration() {
		long interval = 200;
		Flux<String> flux = Flux.interval(Duration.ofMillis(interval))
		                        .map(l -> "foo")
		                        .take(2);

		Duration duration = StepVerifier.create(flux)
		                                .thenAwait(Duration.ofSeconds(100))
		                                .expectNext("foo")
		                                .expectNext("foo")
		                                .expectComplete()
		                                .verify(Duration.ofMillis(500));

		assertThat(duration.toMillis()).isGreaterThan(2 * interval);
	}

	@Test
	public void verifyDurationTimeout() {
		Flux<String> flux = Flux.interval(Duration.ofMillis(200))
		                        .map(l -> "foo")
		                        .take(2);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNext("foo")
		            .expectComplete()
		            .verify(Duration.ofMillis(300)))
	            .withMessageStartingWith("VerifySubscriber timed out on");
	}

	@Test
	public void verifyNever() {
		Flux<String> flux = Flux.never();

		StepVerifier.create(flux)
		            .expectSubscription()
		            .thenCancel()
		            .verify();
	}

	@Test
	public void verifySubscription() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectSubscriptionMatches(s -> s instanceof Fuseable.QueueSubscription)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyNextAs() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar", "foobar"))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyNextAsError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar"))
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectNextSequence\" failed (unexpected iterator request; onNext(foobar); iterable: [foo, bar])");
	}

	@Test
	public void verifyNextAsError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar", "foobar", "bar"))
		            .expectComplete()
		            .verify())
				.withMessageStartingWith("expectation \"expectNextSequence\" failed (")
				.withMessageEndingWith("expected next value: bar; actual signal: onComplete(); iterable: [foo, bar, foobar, bar])");
	}

	@Test
	public void verifyNextAs2() {
		final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux<Integer> flux = Flux.fromStream(source.stream());

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyRecordMatches() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .recordWith(ArrayList::new)
		            .expectNextCount(3)
		            .expectRecordedMatches(c -> c.contains("foobar"))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyRecordMatchesError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .recordWith(ArrayList::new)
		            .expectNextCount(3)
		            .expectRecordedMatches(c -> c.contains("foofoo"))
		            .expectComplete()
		            .verify())
				.withMessage("expectation \"expectRecordedMatches\" failed (expected collection predicate match; actual: [foo, bar, foobar])");
	}

	@Test
	public void verifyRecordNullError() {
		Flux<String> flux = Flux.just("foo", "bar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .recordWith(() -> null)
		            .expectComplete()
		            .verify())
				.withMessage("expectation \"recordWith\" failed (expected collection; actual supplied is [null])");
	}

	@Test
	public void verifyRecordMatchesError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo", "bar", "foobar")
		            .expectRecordedMatches(c -> c.size() == 3)
		            .expectComplete()
		            .verify())
				.withMessage("expectation \"expectRecordedMatches\" failed (expected record collector; actual record is [null])");
	}

	@Test
	public void verifyRecordWith2() {
		final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux<Integer> flux = Flux.fromStream(source.stream());

		StepVerifier.create(flux)
		            .recordWith(ArrayList::new)
		            .expectNextCount(10)
		            .consumeRecordedWith(c -> assertThat(c).containsExactlyElementsOf(source))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifySubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectSubscriptionMatches(s -> false)
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
				.withMessageStartingWith("expectation \"expectSubscriptionMatches\" failed (predicate failed on subscription: ");
	}

	@Test
	public void verifyConsumeSubscription() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .consumeSubscriptionWith(s -> assertThat(s).isInstanceOf(Fuseable.QueueSubscription.class))
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyConsumeSubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .consumeSubscriptionWith(s -> Assertions.fail("boom"))
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
				.withMessage("boom");
	}

	@Test
	public void verifyFusion() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectFusion()
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyFusionError() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> Mono.just("foo")
				                      .hide()
				                      .as(StepVerifier::create)
				                      .expectFusion()
				                      .expectNext("foo")
				                      .expectComplete()
				                      .verify())
				.withMessage("The source publisher does not support fusion");
	}

	@Test
	public void verifyNoFusion() {
		Mono<String> flux = Mono.just("foo")
		                        .hide();

		StepVerifier.create(flux)
		            .expectNoFusionSupport()
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyNoFusionError() {
		Mono<String> flux = Mono.just("foo");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNoFusionSupport()
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
				.withMessage("The source publisher supports fusion");
	}

	@Test
	public void verifyFusionModeRequest() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyFusionModeExpected() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyFusionModeExpectedError() {
		Mono<String> flux = Mono.just("foo");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC, Fuseable.ASYNC)
		            .expectNext("foo")
		            .expectComplete()
		            .verify())
				.withMessage("expectation failed (expected fusion mode: (async); actual: (sync))");
	}

	@Test
	public void verifyFusionModeExpected2() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.immediate());

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC | Fuseable.ASYNC, Fuseable.ASYNC)
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyFusionModeExpected2Error() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.immediate());

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectFusion(Fuseable.ASYNC, Fuseable.SYNC)
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verify())
				.withMessage("expectation failed (expected fusion mode: (sync); actual: (async))");
	}

	@Test
	public void verifyVirtualTimeOnSubscribe() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofDays(2))
		                                       .map(l -> "foo"))
		            .thenAwait(Duration.ofDays(3))
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeOnError() {
		StepVerifier.withVirtualTime(() -> Mono.never()
		                                       .timeout(Duration.ofDays(2))
		                                       .map(l -> "foo"))
		            .thenAwait(Duration.ofDays(2))
		            .expectError(TimeoutException.class)
		            .verify();
	}

	@Test
	public void verifyVirtualTimeNoEvent() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delaySubscription(Duration.ofDays(2)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofDays(2))
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeNoEventError() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delaySubscription(Duration.ofDays(2)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofDays(2))
		            .expectNext("foo")
		            .expectNoEvent(Duration.ofDays(2))
		            .expectComplete()
		            .verify())
				.withMessage("unexpected end during a no-event expectation");
	}

	@Test
	public void verifyVirtualTimeNoEventInterval() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
		                                       .take(2))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3))
		            .expectNext(0L)
		            .expectNoEvent(Duration.ofSeconds(3))
		            .expectNext(1L)
		            .expectComplete()
		            .verify();
	}

	@Test
	//TODO shouldn't there be only one error rather than Multiple exceptions?
	public void verifyVirtualTimeNoEventIntervalError() {
		Throwable thrown = catchThrowable(() ->
				StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
				                                       .take(2))
				            .expectSubscription()
				            .expectNoEvent(Duration.ofSeconds(3))
				            .expectNext(0L)
				            .expectNoEvent(Duration.ofSeconds(4))
				            .expectNext(1L)
				            .thenAwait()
				            .expectComplete()
				            .verify());

		assertThat(thrown).isInstanceOf(AssertionError.class)
		                  .hasMessageContaining("Multiple exceptions")
		                  .hasMessageContaining("expectation failed (expected no event: onNext(1)")
		                  .hasMessageContaining("expectation failed (expected no event: onComplete()");
	}

	@Test
	public void verifyVirtualTimeNoEventNever() {
		StepVerifier.withVirtualTime(() -> Mono.never()
		                                       .log())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofDays(10000))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeNoEventNeverError() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.withVirtualTime(() -> Mono.never()
				                                                         .log())
				                              .expectNoEvent(Duration.ofDays(10000))
				                              .thenCancel()
				                              .verify())
				.withMessageStartingWith("expectation failed (expected no event: onSubscribe(");
	}

	@Test
	public void verifyVirtualTimeOnNext() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo", "bar", "foobar")
		                                       .delay(Duration.ofHours(1))
		                                       .log())
		            .thenAwait(Duration.ofHours(1))
		            .expectNext("foo")
		            .thenAwait(Duration.ofHours(1))
		            .expectNext("bar")
		            .thenAwait(Duration.ofHours(1))
		            .expectNext("foobar")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeOnComplete() {
		StepVerifier.withVirtualTime(() -> Flux.empty()
		                                       .delaySubscription(Duration.ofHours(1))
		                                       .log())
		            .thenAwait(Duration.ofHours(1))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeOnNextInterval() {
		Duration r;

		r = StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
		                                           .map(d -> "t" + d))
		                .thenAwait(Duration.ofSeconds(3))
		                .expectNext("t0")
		                .thenAwait(Duration.ofSeconds(3))
		                .expectNext("t1")
		                .thenAwait(Duration.ofSeconds(3))
		                .expectNext("t2")
		                .thenCancel()
		                .verify();

		assertThat(r.minus(Duration.ofSeconds(9)).toMillis()).isNegative();
	}

	@Test
	public void verifyVirtualTimeNoLookupFails() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> StepVerifier.withVirtualTime(Flux::empty, null, 1))
	            .withMessage("vtsLookup");
	}

	@Test
	public void verifyVirtualTimeNoScenarioFails() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> StepVerifier.withVirtualTime(null, 1))
	            .withMessage("scenarioSupplier");
	}

	@Test(timeout = 3000)
	public void verifyVirtualTimeOnNextIntervalManual() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		StepVerifier.withVirtualTime(() -> Flux.intervalMillis(1000, vts)
		                                       .map(d -> "t" + d))
		            .then(() -> vts.advanceTimeBy(Duration.ofHours(1)))
		            .expectNextCount(3600)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeOnErrorInterval() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
		                                       .map(d -> "t" + d),
				0)
		            .thenRequest(1)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext("t0")
		            .thenRequest(1)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext("t1")
		            .thenAwait(Duration.ofSeconds(3))
		            .expectError(IllegalStateException.class)
		            .verify();

	}

	@Test
	public void verifyVirtualTimeOnErrorAsync() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		StepVerifier.withVirtualTime(() -> Flux.just(123)
		                                       .subscribeOn(vts),
				() -> vts, 0)
		            .thenRequest(1)
		            .thenAwait()
		            .expectNext(123)
		            .expectComplete()
		            .verify();

	}

	@Test(timeout = 1000)
	public void verifyCreatedSchedulerUsesVirtualTime() {
		//a timeout will occur if virtual time isn't used
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
		                                       .map(d -> "t" + d),
				VirtualTimeScheduler::create,
				0)
		            .thenRequest(1)
		            .thenAwait(Duration.ofSeconds(1))
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext("t0")
		            .thenCancel()
		            .verify();
	}

	@Test(timeout = 1000)
	public void verifyCreatedForAllSchedulerUsesVirtualTime() {
		//a timeout will occur if virtual time isn't used
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3))
		                                       .map(d -> "t" + d),
				VirtualTimeScheduler::createForAll,
				0)
		            .thenRequest(1)
		            .thenAwait(Duration.ofSeconds(1))
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext("t0")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void noSignalRealTime() {
		Duration verifyDuration = StepVerifier.create(Mono.never())
		                                      .expectSubscription()
		                                      .expectNoEvent(Duration.ofSeconds(1))
		                                      .thenCancel()
		                                      .verify(Duration.ofMillis(1100));

		assertThat(verifyDuration.toMillis()).isGreaterThanOrEqualTo(1000L);
	}

	@Test(timeout = 500)
	public void noSignalVirtualTime() {
		StepVerifier.withVirtualTime(Mono::never, 1)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(100))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void longDelayAndNoTermination() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo", "bar")
		                                       .delay(Duration.ofSeconds(5))
		                                       .concatWith(Mono.never()),
				Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNext("foo")
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNextCount(1)
		            .expectNoEvent(Duration.ofMillis(10))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void thenAwaitThenCancelWaitsForDuration() {
		Duration verifyDuration = StepVerifier.create(Flux.just("foo", "bar")
		                                                  .delay(Duration.ofMillis(500)))
		                                      .expectSubscription()
		                                      .thenAwait(Duration.ofMillis(500))
		                                      .expectNext("foo")
		                                      .thenAwait(Duration.ofMillis(200))
		                                      .thenCancel()
		                                      .verify(Duration.ofMillis(1000));

		assertThat(verifyDuration.toMillis()).isGreaterThanOrEqualTo(700L);
	}

	@Test
	public void testThenConsumeWhile() {
		StepVerifier.create(Flux.range(3, 8))
		            .expectNextMatches(first -> first == 3)
		            .thenConsumeWhile(v -> v < 9)
		            .expectNext(9)
		            .expectNext(10)
		            .expectComplete()
		            .log()
		            .verify();
	}

	@Test
	public void testThenConsumeWhileWithConsumer() {
		LongAdder count = new LongAdder();

		StepVerifier.create(Flux.range(3, 8))
		            .expectNextMatches(first -> first == 3)
		            .thenConsumeWhile(v -> v < 9, v -> count.increment())
		            .expectNext(9)
		            .expectNext(10)
		            .expectComplete()
		            .log()
		            .verify();

		assertThat(count.intValue()).isEqualTo(5);
	}

	@Test
	public void testThenConsumeWhileFails() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.range(3, 8))
			            .expectNextMatches(first -> first == 3)
			            .thenConsumeWhile(v -> v <= 9)
			            .expectNext(9)
			            .expectNext(10)
			            .expectComplete()
			            .log()
			            .verify())
		        .withMessageContaining("expectNext(9)");
	}

	@Test
	public void testWithDescription() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo", "bar", "baz"), 3)
			            .expectNext("foo")
			            .as("first")
			            .expectNext("bar")
			            .as("second")
			            .expectNext("bar")
			            .as("third")
			            .as("this is ignored")
			            .expectComplete()
			            .log()
			            .verify())
				.withMessageStartingWith("expectation \"third\" failed");
	}

	@Test
	public void noCancelOnUnexpectedErrorSignal() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.error(new IllegalArgumentException())
			                        .doOnCancel(cancelled::increment))
			            .expectComplete()
			            .verify())
				.withMessageContaining("expected: onComplete(); actual: onError");
		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectComplete assertion caused a cancellation")
				.isZero();
	}

	@Test
	public void noCancelOnUnexpectedCompleteSignal() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty()
			                        .doOnCancel(cancelled::increment))
			            .expectError()
			            .verify())
				.withMessageContaining("expected: onError(); actual: onComplete()");
		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectError assertion caused a cancellation")
				.isZero();
	}

	@Test
	public void noCancelOnUnexpectedCompleteSignal2() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
			                        .doOnCancel(cancelled::increment))
			            .expectNext("foo", "bar")
			            .expectComplete()
			            .verify())
		        .withMessageContaining("expected: onNext(bar); actual: onComplete()");

		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectNext assertion caused a cancellation")
	            .isZero();
	}

	@Test
	public void noCancelOnCompleteWhenSequenceUnexpected() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
			                        .doOnCancel(cancelled::increment))
			            .expectNextSequence(Arrays.asList("foo", "bar"))
			            .expectComplete()
			            .verify())
		        .withMessageContaining("expectNextSequence");
		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectNextSequence assertion caused a cancellation")
		        .isZero();
	}

	@Test
	public void noCancelOnCompleteWhenCountUnexpected() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
			                        .doOnCancel(cancelled::increment))
			            .expectNextCount(2)
			            .expectComplete()
			            .verify())
				.withMessageContaining("expectNextCount");

		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectNextCount assertion caused a cancellation")
				.isZero();
	}

	@Test
	public void noCancelOnErrorWhenCollectUnexpected() {
		LongAdder cancelled = new LongAdder();
		LongAdder records = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.error(new IllegalArgumentException())
			                        .doOnCancel(cancelled::increment))
			            .recordWith(() -> {
								records.increment();
								return new ArrayList<>();
			            })
			            .expectRecordedMatches(l -> l.size() == 2)
			            .expectComplete()
			            .verify())
		        .withMessageContaining("expected collection predicate match");

		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectRecordedMatches assertion caused a cancellation")
				.isZero();
		assertThat(records.intValue())
				.as("unexpected number of records")
				.isEqualTo(1);
	}

	//TODO records: find a way to test the case where supplied collection is null, and signal is complete/error
	//TODO records: find a way to test the case where there hasn't been a recorder set, and signal is complete/error

	@Test
	public void cancelOnUnexpectedNextWithMoreData() {
		LongAdder cancelled = new LongAdder();
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo", "bar")
			                        .doOnCancel(cancelled::increment))
			            .expectNext("baz")
			            .expectComplete()
			            .verify())
		        .withMessageContaining("expected value: baz;");

		assertThat(cancelled.intValue())
				.overridingErrorMessage("the expectNext assertion didn't cause a cancellation")
		        .isEqualTo(1);
	}

	@Test
	public void boundedInitialOverflowIsDetected() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(
				REQUEST_OVERFLOW);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(publisher, 1)
				                              .then(() -> publisher.emit("foo", "bar"))
				                              .expectNext("foo")
				                              .expectComplete()
				                              .verify())
				.withMessageStartingWith("request overflow (")
				.withMessageEndingWith("expected production of at most 1;" +
						" produced: 2; request overflown by signal: onNext(bar))");
	}

	@Test
	public void boundedRequestOverflowIsDetected() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(
				REQUEST_OVERFLOW);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(publisher, 0)
			            .thenRequest(2)
			            .then(() -> publisher.emit("foo", "bar", "baz"))
			            .expectNext("foo", "bar")
			            .expectComplete()
			            .verify())
				.withMessageStartingWith("request overflow (")
		        .withMessageEndingWith("expected production of at most 2;"
					+ " produced: 3; request overflown by signal: onNext(baz))");
	}

	@Test
	public void initialBoundedThenUnboundedRequestDoesntOverflow() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(
				REQUEST_OVERFLOW);

		StepVerifier.create(publisher, 2)
		            .thenRequest(Long.MAX_VALUE - 2)
		            .then(() -> publisher.emit("foo", "bar", "baz"))
	                .expectNext("foo", "bar", "baz")
	                .expectComplete()
	                .verify();
	}

	@Test
	public void verifyErrorTriggersVerificationFail() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyError())
		        .withMessage("expectation \"expectError()\" failed (expected: onError(); actual: onComplete())");
	}

	@Test
	public void verifyErrorTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException()))
		            .verifyError();
	}

	@Test
	public void verifyErrorClassTriggersVerificationFail() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyError(IllegalArgumentException.class))
		        .withMessage("expectation \"expectError(Class)\" failed (expected: onError(IllegalArgumentException); actual: onComplete())");
	}

	@Test
	public void verifyErrorClassTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException()))
		            .verifyError(IllegalArgumentException.class);
	}

	@Test
	public void verifyErrorMessageTriggersVerificationFail() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyErrorMessage("boom"))
		        .withMessage("expectation \"expectErrorMessage\" failed (expected: onError(\"boom\"); actual: onComplete())");
	}

	@Test
	public void verifyErrorMessageTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void verifyErrorPredicateTriggersVerificationFail() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyErrorMatches(e -> e instanceof IllegalArgumentException))
		        .withMessage("expectation \"expectErrorMatches\" failed (expected: onError(); actual: onComplete())");
	}

	@Test
	public void verifyErrorPredicateTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException);
	}

	@Test
	public void verifyCompleteTriggersVerificationFail() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.error(new IllegalArgumentException()))
				                              .verifyComplete())
		        .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onError(java.lang.IllegalArgumentException))");
	}

	@Test
	public void verifyCompleteTriggersVerificationSuccess() {
			StepVerifier.create(Flux.just(1, 2))
			            .expectNext(1, 2)
		                .verifyComplete();
	}

}
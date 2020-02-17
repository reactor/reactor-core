/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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
package reactor.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.context.Context;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.*;
import static reactor.test.publisher.TestPublisher.Violation.REQUEST_OVERFLOW;

/**
 * @author Arjen Poutsma
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @author Simon Basle
 */
public class StepVerifierTests {

	@Test
	public void expectationErrorWithGenericValueFormatterBypassesExtractor() {
		Flux<String> flux = Flux.just("foobar");
		StepVerifierOptions options = StepVerifierOptions
				.create()
				.valueFormatter(ValueFormatters.forClass(Object.class, t -> t.getClass().getSimpleName() + "{'" + t + "'}"));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.create(flux, options)
				                        .expectError()
						::verify)
				.withMessage("expectation \"expectError()\" failed (expected: onError(); actual: ImmutableSignal{'onNext(foobar)'})");
	}

	@Test
	public void expectationErrorWithSpecificValueFormatterExtractsSignal() {
		Flux<String> flux = Flux.just("foobar");
		StepVerifierOptions options = StepVerifierOptions
				.create()
				.valueFormatter(ValueFormatters.forClass(String.class, s -> "" + s.length()));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.create(flux, options)
				                        .expectError()
						::verify)
				.withMessage("expectation \"expectError()\" failed (expected: onError(); actual: onNext(6))");
	}

	@Test
	public void expectationErrorWithoutValueFormatter() {
		Flux<String> flux = Flux.just("foobar");
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.create(flux, options)
				                        .expectError()
						::verify)
				.withMessage("expectation \"expectError()\" failed (expected: onError(); actual: onNext(foobar))");
	}

	@Test
	public void expectInvalidNextsWithCustomConverter() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifierOptions options = StepVerifierOptions.create()
		                                                 .valueFormatter(ValueFormatters.forClass(String.class, s -> "String=>" + s));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux, options)
				                              .expectNext("foo", "baz")
				                              .expectComplete()
				                              .verify())
				.withMessage("expectation \"expectNext(String=>baz)\" failed (expected value: String=>baz; actual value: String=>bar)");
	}

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
	public void expectNextsMoreThan6() {
		Flux<Integer> flux = Flux.range(1, 7);

		StepVerifier.create(flux)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
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
	public void assertNext() throws Exception {
		Flux<String> flux = Flux.just("foo");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
				                              .assertNext(s -> assertThat(s).endsWith("ooz"))
				                              .expectComplete()
				                              .verify())
				.withMessage("\nExpecting:\n <\"foo\">\nto end with:\n <\"ooz\">\n");
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
	public void expectNextCountZeroBeforeExpectNext() {
		StepVerifier.create(Flux.just("foo", "bar"))
				.expectNextCount(0)
				.expectNext("foo", "bar")
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
		            .thenRequest(Integer.MAX_VALUE)
		            .expectNextCount(900_001)
		            .expectComplete()
		            .verify())
	            .withMessageStartingWith("expectation \"expectNextCount(900001)\" failed")
				.withMessageContaining("expected: count = 900001; actual: counted = 900000; signal: onComplete()");
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

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo", "bar")
		            .expectNextCount(2)
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectNextCount(2)\" failed (expected: count = 2; actual: counted = 0; signal: onComplete())");
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
				.withMessage("expectation \"expectNextCount(4)\" failed (expected: count = 4; actual: counted = 2; signal: onComplete())");
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
	public void errorSatisfies() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorSatisfies(t -> assertThat(t).isInstanceOf(IllegalArgumentException.class))
		            .verify();
	}

	@Test
	public void errorSatisfiesInvalid() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorSatisfies(t -> assertThat(t).hasMessage("foo"))
		            .verify())
	            .withMessage("expectation \"expectErrorSatisfies\" failed (assertion failed on exception <java.lang.IllegalArgumentException>: " +
			            "\nExpecting message:\n <\"foo\">\nbut was:\n <null>)");
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
	public void thenCancel_cancelsAfterFirst() {
		TestPublisher<Long> publisher = TestPublisher.create();
		AtomicBoolean downStreamCancelled = new AtomicBoolean();
		AtomicBoolean asserted = new AtomicBoolean();
		Flux<Long> source = publisher
				.flux()
				.onBackpressureBuffer()
				.doOnCancel(() -> downStreamCancelled.set(true))
				.log();

		Duration took = StepVerifier.create(source,  1)
		                            .then(() -> Schedulers.elastic().schedule(() -> publisher.next(0L)))
		                            .assertNext(next -> {
			                            LockSupport.parkNanos(Duration.ofMillis(500)
			                                                          .toNanos());
			                            asserted.set(true);
			                            assertThat(next).isEqualTo(0L);
		                            })
		                            .then(() -> Schedulers.elastic().schedule(() ->
				                            publisher.next(1L)))
		                            .then(() -> Schedulers.elastic().schedule(() ->
				                            publisher.next(2L), 50, TimeUnit.MILLISECONDS))
		                            .expectNoEvent(Duration.ofMillis(100))
		                            .thenRequest(1)
		                            .thenRequest(1)
		                            .assertNext(next -> {
			                            LockSupport.parkNanos(Duration.ofMillis(500)
			                                                          .toNanos());
			                            assertThat(next).isEqualTo(1L);
		                            })
		                            .thenAwait(Duration.ofSeconds(2))
		                            .thenCancel()
		                            .verify(Duration.ofSeconds(5));

		publisher.assertCancelled();

		assertThat(asserted.get())
				.as("expectation processed")
				.isTrue();
		assertThat(downStreamCancelled.get())
				.as("is cancelled by awaitThenCancel")
				.isTrue();
		assertThat(took.toMillis())
				.as("blocked on first assertNext")
				.isGreaterThanOrEqualTo(1000L);
	}

	@Test
	public void thenCancel_cancelsAfterFirst2() {
		TestPublisher<Long> publisher = TestPublisher.create();
		AtomicBoolean downStreamCancelled = new AtomicBoolean();
		AtomicBoolean asserted = new AtomicBoolean();
		Flux<Long> source = publisher
				.flux()
				.doOnCancel(() -> downStreamCancelled.set(true));

		Duration took = StepVerifier.create(source)
		                            .then(() -> Schedulers.elastic().schedule(() -> publisher.next(0L)))
		                            .assertNext(next -> {
			                            asserted.set(true);
			                            assertThat(next).isEqualTo(0L);
		                            })
		                            .then(() -> Schedulers.elastic().schedule(() ->
				                            publisher.next(1L)))
		                            .thenCancel()
		                            .verify(Duration.ofSeconds(5));

		publisher.assertCancelled();

		assertThat(asserted.get())
				.as("expectation processed")
				.isTrue();
		assertThat(downStreamCancelled.get())
				.as("is cancelled by awaitThenCancel")
				.isTrue();
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
	public void verifyNeverWithExpectTimeout() {
		Flux<String> flux = Flux.never();

		StepVerifier.create(flux)
		            .expectTimeout(Duration.ofMillis(500))
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
	public void verifyNextAsErrorTooFewInIterable() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar"))
		            .expectComplete()
		            .verify())
	            .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onNext(foobar))");
	}

	@Test
	public void verifyNextAsErrorTooManyInIterable() {
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
	public void verifyNextAsWithEmptyFlux() {
	    final List<Integer> source = Arrays.asList(1,2,3);
		Flux<Integer> flux = Flux.empty();

        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> StepVerifier.create(flux)
				.expectNextSequence(source)
				.expectComplete()
				.verify())
                .withMessageStartingWith("expectation \"expectNextSequence\" failed (")
                .withMessageEndingWith("expected next value: 1; actual signal: onComplete(); iterable: [1, 2, 3])");
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
	public void verifyConsumeSubscriptionAfterFirst() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .consumeSubscriptionWith(s -> assertThat(s).isInstanceOf(Fuseable.QueueSubscription.class))
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
				.withMessage("expectation failed (expected fuseable source but actual " +
						"Subscription is not: 3)");
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

		StepVerifier.create(flux.hide())
		            .expectNoFusionSupport()
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
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
	public void verifyFusionModeExpectedCancel() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC)
		            .expectNext("foo")
		            .thenCancel()
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
				.withMessage("Unexpected completion during a no-event expectation");
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

	//see https://github.com/reactor/reactor-core/issues/1913
	@Test
	public void verifyExpectTimeoutFailsWhenSomeEvent() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Mono.just("foo"))
				                              .expectTimeout(Duration.ofMillis(1300))
				                              .verify())
				.withMessage("expectation \"expectTimeout\" failed (expected: timeout(1.3s); actual: onNext(foo))");
	}

	//see https://github.com/reactor/reactor-core/issues/1913
	@Test
	public void verifyVirtualTimeExpectTimeoutFailsWhenSomeEvent() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.withVirtualTime(() -> Mono.just("foo"))
				                              .expectTimeout(Duration.ofDays(3))
				                              .verify())
				.withMessage("expectation \"expectTimeout\" failed (expected: timeout(72h); actual: onNext(foo))");
	}

	@Test
	public void verifyExpectTimeoutNever() {
		StepVerifier.create(Mono.never())
		            .expectSubscription()
		            .expectTimeout(Duration.ofSeconds(1))
		            .verify();
	}

	@Test
	public void verifyVirtualTimeExpectTimeoutNever() {
		StepVerifier.withVirtualTime(Mono::never)
		            .expectSubscription()
		            .expectTimeout(Duration.ofDays(10000))
		            .verify();
	}

	@Test
	public void verifyExpectTimeoutDoesntCareAboutSubscription() {
		StepVerifier.withVirtualTime(Mono::never)
		            .expectTimeout(Duration.ofDays(10000))
		            .verify();

		StepVerifier.create(Mono.never())
		            .expectTimeout(Duration.ofSeconds(1))
		            .verify();
	}

	@Test
	public void verifyVirtualTimeOnNext() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo", "bar", "foobar")
		                                       .delayElements(Duration.ofHours(1))
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

		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofMillis(1000), vts)
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
	public void verifyCreatedForAllSchedulerUsesVirtualTime() {
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

	@Test
	public void noSignalRealTime() {
		Duration verifyDuration = StepVerifier.create(Mono.never())
		                                      .expectSubscription()
		                                      .expectTimeout(Duration.ofSeconds(1))
		                                      .verify(Duration.ofMillis(1100));

		assertThat(verifyDuration.toMillis()).isGreaterThanOrEqualTo(1000L);
	}

	@Test(timeout = 500)
	public void noSignalVirtualTime() {
		StepVerifier.withVirtualTime(Mono::never, 1)
		            .expectSubscription()
		            .expectTimeout(Duration.ofSeconds(100))
		            .verify();
	}

	@Test
	public void longDelayAndNoTermination() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo", "bar")
		                                       .delayElements(Duration.ofSeconds(5))
		                                       .concatWith(Mono.never()),
				Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNext("foo")
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNextCount(1)
		            .expectTimeout(Duration.ofHours(10))
		            .verify();
	}

	//source: https://stackoverflow.com/questions/58486417/how-to-verify-with-stepverifier-that-provided-mono-did-not-completed
	@Test
	public void expectTimeoutSmokeTest() {
		Mono<String> neverMono = Mono.never();
		Mono<String> completingMono = Mono.empty();

		StepVerifier.create(neverMono, StepVerifierOptions.create().scenarioName("neverMono should pass"))
				.expectTimeout(Duration.ofSeconds(1))
				.verify();

		StepVerifier shouldFail = StepVerifier.create(completingMono).expectTimeout(Duration.ofSeconds(1));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(shouldFail::verify)
				.withMessage("expectation \"expectTimeout\" failed (expected: timeout(1s); actual: onComplete())");
	}

	@Test
	public void verifyTimeoutSmokeTest() {
		Mono<String> neverMono = Mono.never();
		Mono<String> completingMono = Mono.empty();

		StepVerifier.create(neverMono, StepVerifierOptions.create().scenarioName("neverMono should pass"))
				.verifyTimeout(Duration.ofSeconds(1));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(completingMono).verifyTimeout(Duration.ofSeconds(1)))
				.withMessage("expectation \"expectTimeout\" failed (expected: timeout(1s); actual: onComplete())");
	}

	@Test
	public void thenAwaitThenCancelWaitsForDuration() {
		Duration verifyDuration = StepVerifier.create(Flux.just("foo", "bar")
		                                                  .delayElements(Duration.ofMillis(500)))
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
	public void testExpectRecordedMatches() {
		List<Integer> expected = Arrays.asList(1,2);

		StepVerifier.create(Flux.just(1,2))
		            .recordWith(ArrayList::new)
		            .thenConsumeWhile(i -> i < 2)
		            .expectRecordedMatches(expected::equals)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void testExpectRecordedMatchesTwice() {
		List<Integer> expected1 = Arrays.asList(1,2);
		List<Integer> expected2 = Arrays.asList(3,4);

		StepVerifier.create(Flux.just(1,2,3,4))
		            .recordWith(ArrayList::new)
		            .thenConsumeWhile(i -> i < 2)
		            .expectRecordedMatches(expected1::equals)
		            .recordWith(ArrayList::new)
		            .thenConsumeWhile(i -> i < 4)
		            .expectRecordedMatches(expected2::equals)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void testExpectRecordedMatchesWithoutComplete() {
		List<Integer> expected = Arrays.asList(1,2);

		TestPublisher<Integer> publisher = TestPublisher.createCold();
		publisher.next(1);
		publisher.next(2);

		StepVerifier.create(publisher)
		            .recordWith(ArrayList::new)
		            .thenConsumeWhile(i -> i < 2)
		            .expectRecordedMatches(expected::equals)
		            .thenCancel()
		            .verify();
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
	public void testWithDescriptionAndScenarioName() {
		StepVerifierOptions options = StepVerifierOptions.create()
		                                                 .initialRequest(3)
		                                                 .scenarioName("some scenario name");
		StepVerifier stepVerifier = StepVerifier
				.create(Flux.just("foo", "bar", "baz"), options)
				.expectNext("foo")
				.as("first")
				.expectNext("bar")
				.as("second")
				.expectNext("bar")
				.as("third")
				.as("this is ignored")
				.expectComplete()
				.log();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(stepVerifier::verify)
				.withMessage("[some scenario name] expectation \"third\" failed (expected value: bar; actual value: baz)");
	}

	@Test
	public void testDurationFailureWithScenarioName() {
		StepVerifierOptions options = StepVerifierOptions.create()
		                                                 .scenarioName("some scenario name");
		StepVerifier stepVerifier = StepVerifier
				.create(Mono.delay(Duration.ofMillis(100)), options)
				.expectNextCount(1)
				.expectComplete();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> stepVerifier.verify(Duration.ofMillis(10)))
				.withMessageStartingWith("[some scenario name] VerifySubscriber timed out on reactor.core.publisher.MonoDelay$MonoDelayRunnable@");
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
	public void verifyErrorPredicateTriggersVerificationFailBadSignal() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyErrorMatches(e -> e instanceof IllegalArgumentException))
		        .withMessage("expectation \"expectErrorMatches\" failed (expected: onError(); actual: onComplete())");
	}

	@Test
	public void verifyErrorPredicateTriggersVerificationFailNoMatch() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
				                              .verifyErrorMatches(e -> e.getMessage() == null))
		        .withMessage("expectation \"expectErrorMatches\" failed (predicate failed on exception: java.lang.IllegalArgumentException: boom)");
	}

	@Test
	public void verifyErrorPredicateTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException);
	}

	@Test
	public void verifyErrorAssertionTriggersVerificationFailBadSignal() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.empty())
				                              .verifyErrorSatisfies(e -> assertThat(e).isNotNull()))
		        .withMessage("expectation \"verifyErrorSatisfies\" failed (expected: onError(); actual: onComplete())");
	}

	@Test
	public void verifyErrorAssertionTriggersVerificationFailNoMatch() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
				                              .verifyErrorSatisfies(e -> assertThat(e).hasMessage("foo")))
		        .withMessage("expectation \"verifyErrorSatisfies\" failed (assertion failed on exception <java.lang.IllegalArgumentException: boom>: "
				        + "\nExpecting message:\n <\"foo\">\nbut was:\n <\"boom\">)");
	}

	@Test
	public void verifyErrorAssertionTriggersVerificationSuccess() {
		StepVerifier.create(Flux.error(new IllegalArgumentException("boom")))
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("boom"));
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

	@Test
	public void expectNextCountAfterExpectNext() {
		StepVerifier.create(Flux.range(1, 5))
	                .expectNext(1, 2)
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextCountAfterThenConsumeWhile() {
		StepVerifier.create(Flux.range(1, 5).log())
	                .thenConsumeWhile(i -> i <= 2)
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextCountAfterExpectNextCount() {
		StepVerifier.create(Flux.range(1, 5))
	                .expectNextCount(2)
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextCountAfterExpectNextMatches() {
		StepVerifier.create(Flux.range(1, 5))
	                .expectNextMatches(i -> true)
	                .expectNextMatches(i -> true)
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextCountAfterExpectNextSequence() {
		StepVerifier.create(Flux.range(1, 5))
	                .expectNextSequence(Arrays.asList(1, 2))
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextCountAfterConsumeNextWith() {
		StepVerifier.create(Flux.range(1, 5))
	                .consumeNextWith(i -> {})
	                .consumeNextWith(i -> {})
	                .expectNextCount(3)
	                .verifyComplete();
	}

	@Test
	public void expectNextSequenceWithPartialMatchingSequence() {
		StepVerifier.create(Flux.range(1, 5))
	                .expectNextSequence(Arrays.asList(1, 2, 3))
	                .expectNext(4, 5)
	                .verifyComplete();
	}

	@Test
	public void expectNextSequenceWithPartialMatchingSequenceNoMoreExpectation() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.range(1, 5))
	                .expectNextSequence(Arrays.asList(1, 2, 3))
	                .verifyComplete())
	            .withMessage("expectation \"expectComplete\" failed (expected: onComplete(); actual: onNext(4))");
	}

	@Test
	public void expectNextSequenceEmptyListBeforeExpectNext() {
		StepVerifier.create(Flux.just("foo", "bar"))
				.expectNextSequence(emptyList())
				.expectNext("foo", "bar")
				.expectComplete()
				.verify();
	}

	@Test
	public void expectNextErrorIsSuppressed() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
				                                          .flatMap(r -> { throw new ArrayIndexOutOfBoundsException();}))
	                .expectNext("foo")
	                .verifyError())
				.satisfies(error -> {
					assertThat(error)
							.hasMessageStartingWith("expectation \"expectNext(foo)\" failed")
							.hasMessageContaining("actual: onError(java.lang.ArrayIndexOutOfBoundsException)");
					assertThat(error.getSuppressed())
							.hasSize(1)
							.allMatch(spr -> spr instanceof ArrayIndexOutOfBoundsException);
				});
	}

	@Test
	public void consumeNextErrorIsSuppressed() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
				.flatMap(r -> { throw new ArrayIndexOutOfBoundsException();}))
	                .consumeNextWith(v -> assertThat(v).isNotNull())
	                .verifyError())
				.satisfies(error -> {
					assertThat(error)
							.hasMessageStartingWith("expectation \"consumeNextWith\" failed")
							.hasMessageContaining("actual: onError(java.lang.ArrayIndexOutOfBoundsException)");
					assertThat(error.getSuppressed())
							.hasSize(1)
							.allMatch(spr -> spr instanceof ArrayIndexOutOfBoundsException);
				});
	}

	@Test
	public void expectNextCountErrorIsSuppressed() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
				.flatMap(r -> { throw new ArrayIndexOutOfBoundsException();}))
	                .expectNextCount(1)
	                .verifyError())
				.satisfies(error -> {
					assertThat(error)
							.hasMessageStartingWith("expectation \"expectNextCount(1)\" failed")
							.hasMessageContaining("signal: onError(java.lang.ArrayIndexOutOfBoundsException)");
					assertThat(error.getSuppressed())
							.hasSize(1)
							.allMatch(spr -> spr instanceof ArrayIndexOutOfBoundsException);
				});
	}

	@Test
	public void expectNextSequenceErrorIsSuppressed() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo")
				.flatMap(r -> { throw new ArrayIndexOutOfBoundsException();}))
	                .expectNextSequence(Arrays.asList("foo"))
	                .verifyError())
				.satisfies(error -> {
					assertThat(error)
							.hasMessageStartingWith("expectation \"expectNextSequence\" failed")
							.hasMessageContaining("actual signal: onError(java.lang.ArrayIndexOutOfBoundsException)");
					assertThat(error.getSuppressed())
							.hasSize(1)
							.allMatch(spr -> spr instanceof ArrayIndexOutOfBoundsException);
				});
	}

	@Test
	public void consumeWhileErrorIsSuppressed() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo", "bar", "foobar")
		                        .map(r -> { if (r.length() > 3) throw new ArrayIndexOutOfBoundsException(); return r;}))
	                .thenConsumeWhile(s -> s.length() <= 3) //doesn't fail by itself...
	                .verifyComplete()) //...so this will fail
				.satisfies(error -> {
					assertThat(error)
							.hasMessageStartingWith("expectation \"expectComplete\" failed")
							.hasMessageContaining("actual: onError(java.lang.ArrayIndexOutOfBoundsException)");
					assertThat(error.getSuppressed())
							.hasSize(1)
							.allMatch(spr -> spr instanceof ArrayIndexOutOfBoundsException);
				});
	}

	@Test
	public void requestBufferDoesntOverflow() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime(//start with a request for 1 buffer
				() -> source.bufferUntil(i -> i % 3 == 0), 1)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(2)
		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(3)
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify();

		//see same pattern in reactor.core.publisher.FluxBufferPredicateTest.requestBounded
		assertThat(requestCallCount.intValue()).isEqualTo(9L);
		assertThat(totalRequest.longValue()).isEqualTo(12L);
	}

	@Test(timeout = 1000L)
	public void expectCancelDoNotHang() {
		StepVerifier.create(Flux.just("foo", "bar"), 1)
		            .expectNext("foo")
		            .thenCancel()
		            .verify();
	}

	@Test(timeout = 1000L)
	public void consumeNextWithLowRequestShortcircuits() {
		StepVerifier.Step<String> validSoFar = StepVerifier.create(Flux.just("foo", "bar"), 1)
				                         .expectNext("foo");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.consumeNextWith(s -> {}))
	            .withMessageStartingWith("The scenario will hang at consumeNextWith due to too little request being performed for the expectations to finish")
	            .withMessageEndingWith("request remaining since last step: 0, expected: 1");
	}

	@Test(timeout = 1000L)
	public void assertNextLowRequestShortcircuits() {
		StepVerifier.Step<String> validSoFar = StepVerifier.create(Flux.just("foo", "bar"), 1)
		                                                   .expectNext("foo");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.assertNext(s -> {}))
				.withMessageStartingWith("The scenario will hang at assertNext due to too little request being performed for the expectations to finish")
				.withMessageEndingWith("request remaining since last step: 0, expected: 1");
	}

	@Test(timeout = 1000L)
	public void expectNextLowRequestShortcircuits() {
		StepVerifier.Step<String> validSoFar = StepVerifier.create(Flux.just("foo", "bar"), 1)
		                                                   .expectNext("foo");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.expectNext("bar"))
				.withMessageStartingWith("The scenario will hang at expectNext(bar) due to too little request being performed for the expectations to finish")
				.withMessageEndingWith("request remaining since last step: 0, expected: 1");
	}

	@Test(timeout = 1000L)
	public void expectNextCountLowRequestShortcircuits() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> StepVerifier.create(Flux.just("foo", "bar"), 1)
				                              .expectNextCount(2)
				)
	            .withMessageStartingWith("The scenario will hang at expectNextCount(2) due to too little request being performed for the expectations to finish; ")
				.withMessageEndingWith("request remaining since last step: 1, expected: 2");
	}

	@Test(timeout = 1000L)
	public void expectNextMatchesLowRequestShortcircuits() {
		StepVerifier.Step<String> validSoFar = StepVerifier.create(Flux.just("foo", "bar"), 1)
		                                                   .expectNext("foo");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.expectNextMatches("bar"::equals))
				.withMessageStartingWith("The scenario will hang at expectNextMatches due to too little request being performed for the expectations to finish")
				.withMessageEndingWith("request remaining since last step: 0, expected: 1");
	}

	@Test(timeout = 1000L)
	public void expectNextSequenceLowRequestShortcircuits() {
		StepVerifier.Step<String> validSoFar = StepVerifier.create(Flux.just("foo", "bar"), 1);
		List<String> expected = Arrays.asList("foo", "bar");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.expectNextSequence(expected))
				.withMessageStartingWith("The scenario will hang at expectNextSequence due to too little request being performed for the expectations to finish")
				.withMessageEndingWith("request remaining since last step: 1, expected: 2");
	}

	@Test(timeout = 1000L)
	public void thenConsumeWhileLowRequestShortcircuits() {
		StepVerifier.Step<Integer> validSoFar = StepVerifier.create(Flux.just(1, 2), 1)
		                                                    .expectNext(1);

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> validSoFar.thenConsumeWhile(s -> s == 1))
				.withMessageStartingWith("The scenario will hang at thenConsumeWhile due to too little request being performed for the expectations to finish; ")
	            .withMessageEndingWith("request remaining since last step: 0, expected: at least 1 (best effort estimation)");
	}

	@Test(timeout = 1000L)
	public void lowRequestCheckCanBeDisabled() {
		StepVerifier.create(Flux.just(1, 2),
				StepVerifierOptions.create().initialRequest(1).checkUnderRequesting(false))
		            .expectNext(1)
		            .thenConsumeWhile(s -> s == 1); //don't verify, this alone would throw an exception if check activated
	}

	@Test
	public void takeAsyncFusedBackpressured() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.take(3), 0)
		            .expectFusion()
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void cancelAsyncFusion() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.take(3), 0)
		            .expectFusion()
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenCancel()
					.verify();
	}

	@Test
	public void virtualTimeSchedulerUseExactlySupplied() {
		VirtualTimeScheduler vts1 = VirtualTimeScheduler.create();
		VirtualTimeScheduler vts2 = VirtualTimeScheduler.create();
		VirtualTimeScheduler.getOrSet(vts1);

		StepVerifier.withVirtualTime(Mono::empty, () -> vts2, Long.MAX_VALUE)
		            .then(() -> assertThat(VirtualTimeScheduler.get()).isSameAs(vts2))
		            .verifyComplete();

		assertThat(vts1.isDisposed()).isFalse();
		assertThat(vts2.isDisposed()).isTrue();
		assertThat(VirtualTimeScheduler.isFactoryEnabled()).isFalse();
	}

	@Test
	public void virtualTimeSchedulerVeryLong() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofMillis(1))
		                                       .map(tick -> new Date())
		                                       .take(100000)
		                                       .collectList())
		            .thenAwait(Duration.ofHours(1000))
		            .consumeNextWith(list -> Assert.assertTrue(list.size() == 100000))
		            .verifyComplete();
	}

	@Test
	public void withInitialContext() {
		StepVerifier.create(Mono.subscriberContext(),
				StepVerifierOptions.create().withInitialContext(Context.of("foo", "bar")))
		            .assertNext(c -> Assertions.assertThat(c.getOrDefault("foo", "baz"))
		                                       .isEqualTo("bar"))
		            .verifyComplete();
	}

	@Test
	public void withInitialContextButNoPropagation() {
		StepVerifier.create(Mono.just(1), //just(1) uses a ScalarSubscription which can't be resolved to a chain of parents
				StepVerifierOptions.create().withInitialContext(Context.of("foo", "bar")))
		            .expectNoAccessibleContext()
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void withInitialContextAndContextAssertionsParents() {
		StepVerifier.create(Mono.just(1).map(i -> i + 10), //this causes the subscription to be resolvable to a chain of parents
				StepVerifierOptions.create().withInitialContext(Context.of("foo", "bar")))
		            .expectAccessibleContext()
		            .contains("foo", "bar")
		            .then()
		            .expectNext(11)
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/959
	@Test
	public void assertNextWithSubscribeOnDirectProcessor() {
		Scheduler scheduler = Schedulers.newBoundedElastic(1, 100, "test");
		DirectProcessor<Integer> processor = DirectProcessor.create();
		Mono<Integer> doAction = Mono.fromSupplier(() -> 22)
		                             .doOnNext(processor::onNext)
		                             .subscribeOn(scheduler);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(
						StepVerifier.create(processor)
						            .then(doAction::subscribe)
						            .assertNext(v -> assertThat(v).isEqualTo(23))
						            .thenCancel()
								::verify);
	}

	//see https://github.com/reactor/reactor-core/issues/959
	@Test
	public void assertNextWithSubscribeOnJust() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(
						StepVerifier.create(Flux.just(1)
						                        .subscribeOn(Schedulers.newSingle("test")))
						            .then(() -> System.out.println("foo"))
						            .assertNext(v -> assertThat(v).isNull())
						            .thenCancel()
								::verify);
	}

	@Test
	public void parallelVerifyWithVtsMutuallyExclusive() {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		for (int i = 0; i < 10; i++) {
			Future<Duration> ex1 = executorService.submit(() -> StepVerifier
					.withVirtualTime(() -> Flux.just("A", "B", "C")
					                           .delaySequence(Duration.ofMillis(100))
					)
					.then(() -> {
						try {
							Thread.sleep(100);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
					})
					.thenAwait(Duration.ofMillis(100))
					.expectNextCount(3)
					.verifyComplete());

			Future<Duration> ex2 = executorService.submit(() -> StepVerifier
					.withVirtualTime(() -> Flux.just(1, 2, 3)
					                           .delaySequence(Duration.ofMillis(100))
					)
					.thenAwait(Duration.ofMillis(100))
					.expectNextCount(3)
					.expectComplete()
					.verify());

			assertThatCode(ex1::get).as("execution 1 in iteration #" + i).doesNotThrowAnyException();
			assertThatCode(ex2::get).as("execution 2 in iteration #" + i).doesNotThrowAnyException();
		}
	}

	@Test(timeout = 5000)
	public void gh783() {
		int size = 1;
		Scheduler parallel = Schedulers.newParallel("gh-783");
		StepVerifier.withVirtualTime(() -> Flux.just("Oops")
		                                       .take(size)
		                                       .subscribeOn(parallel)
		                                       .flatMap(message -> {
			                                       Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
			                                       return interval.map( tick -> message);
		                                       })
		                                       .take(size)
		                                       .collectList()
		)
		            .thenAwait(Duration.ofHours(1))
		            .consumeNextWith(list -> assertThat(list).hasSize(size))
		            .verifyComplete();
	}

	@Test(timeout = 5000)
	public void gh783_deferredAdvanceTime() {
		int size = 61;
		Scheduler parallel = Schedulers.newParallel("gh-783");
		StepVerifier.withVirtualTime(() -> Flux.range(1, 10)
		                                       .take(size)
		                                       .subscribeOn(parallel)
		                                       .flatMap(message -> {
			                                       Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
			                                       return interval.map( tick -> message);
		                                       }, 30,1)
		                                       .take(size)
		                                       .collectList()
		)
		            .thenAwait(Duration.ofHours(2))
		            .consumeNextWith(list -> assertThat(list).hasSize(size))
		            .expectComplete()
		            .verify();
	}

	@Test
	@Ignore
	//FIXME this case of doubly-nested schedules is still not fully fixed
	public void gh783_withInnerFlatmap() {
		int size = 61;
		Scheduler parallel = Schedulers.newParallel("gh-783");
		StepVerifier.withVirtualTime(() -> Flux.range(1, 10)
		                                       .take(size)
		                                       .subscribeOn(parallel)
		                                       .flatMap(message -> {
			                                       Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
			                                       return interval.flatMap( tick -> Mono.delay(Duration.ofMillis(500))
			                                                                            .thenReturn(message)
			                                                                            .subscribeOn(parallel))
			                                                      .subscribeOn(parallel);
		                                       }, 1,30)
		                                       .take(size)
		                                       .collectList()
		)
		            .thenAwait(Duration.ofMillis(1500 * (size + 10)))
		            .consumeNextWith(list -> assertThat(list).hasSize(size))
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

	@Test
	public void gh783_intervalFullyEmitted() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo").flatMap(message -> Flux.interval(Duration.ofMinutes(5)).take(12)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMinutes(5))
		            .expectNext(0L)
		            .thenAwait(Duration.ofMinutes(25))
		            .expectNext(1L, 2L, 3L, 4L, 5L)
		            .thenAwait(Duration.ofMinutes(30))
		            .expectNext(6L, 7L, 8L, 9L, 10L, 11L)
		            .expectComplete()
		            .verify(Duration.ofMillis(500));
	}

	@Test
	public void gh783_firstSmallAdvance() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo").flatMap(message -> Flux.interval(Duration.ofMinutes(5)).take(12)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMinutes(3))
		            .thenAwait(Duration.ofHours(1))
		            .expectNextCount(12)
		            .expectComplete()
		            .verify(Duration.ofMillis(500));
	}

	@Test
	public void noEventExpectationButComplete() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.create(Flux.empty().hide())
				                        .expectSubscription()
				                        .expectNoEvent(Duration.ofMillis(50))
				                        .expectComplete()
						::verify)
				.withMessage("Unexpected completion during a no-event expectation");
	}

	@Test
	public void noEventExpectationButError() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.create(Flux.error(new IllegalStateException("boom")).hide())
				                        .expectSubscription()
				                        .expectNoEvent(Duration.ofMillis(50))
				                        .expectComplete()
						::verify)
				.withMessage("Unexpected error during a no-event expectation: java.lang.IllegalStateException: boom")
				.withCause(new IllegalStateException("boom"));
	}

	@Test
	public void virtualTimeNoEventExpectationButComplete() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.withVirtualTime(() -> Flux.empty().hide())
				                        .expectSubscription()
				                        .expectNoEvent(Duration.ofMillis(50))
				                        .expectComplete()
						::verify)
				.withMessage("Unexpected completion during a no-event expectation");
	}

	@Test
	public void virtualTimeNoEventExpectationButError() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(StepVerifier.withVirtualTime(() -> Flux.error(new IllegalStateException("boom")).hide())
				                        .expectSubscription()
				                        .expectNoEvent(Duration.ofMillis(50))
				                        .expectComplete()
						::verify)
				.withMessage("Unexpected error during a no-event expectation: java.lang.IllegalStateException: boom")
				.withCause(new IllegalStateException("boom"));
	}

	@Test
	public void verifyLaterCanVerifyConnectableFlux() {
		Flux<Integer> autoconnectableFlux = Flux.just(1, 2, 3).publish().autoConnect(2);

		StepVerifier deferred1 = StepVerifier.create(autoconnectableFlux)
		                                     .expectNext(1, 2, 3)
		                                     .expectComplete()
		                                     .verifyLater();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> deferred1.verify(Duration.ofSeconds(1)))
				.withMessageContaining("timed out");

		StepVerifier deferred2 = StepVerifier.create(autoconnectableFlux)
		                                     .expectNext(1, 2, 3)
		                                     .expectComplete()
		                                     .verifyLater()
		                                     .verifyLater()
		                                     .verifyLater()
		                                     .verifyLater();

		deferred1.verify(Duration.ofSeconds(1));
		deferred2.verify(Duration.ofSeconds(1));
	}

	@Test
	public void verifyLaterCanVerifyConnectableFlux_withAssertionErrors() {
		Flux<Integer> autoconnectableFlux = Flux.just(1, 2, 3).publish().autoConnect(2);

		StepVerifier deferred1 = StepVerifier.create(autoconnectableFlux)
		                                     .expectNext(1, 2, 4)
		                                     .expectComplete()
		                                     .verifyLater();

		StepVerifier deferred2 = StepVerifier.create(autoconnectableFlux)
		                                     .expectNext(1, 2, 5)
		                                     .expectComplete()
		                                     .verifyLater();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> deferred1.verify(Duration.ofSeconds(10)))
				.withMessage("expectation \"expectNext(4)\" failed (expected value: 4; actual value: 3)");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> deferred2.verify(Duration.ofSeconds(10)))
				.withMessage("expectation \"expectNext(5)\" failed (expected value: 5; actual value: 3)");
	}

	@Test
	public void verifyDrainOnRequestInCaseOfFusion() {
		MonoProcessor<Integer> processor = MonoProcessor.create();
		StepVerifier.create(processor, 0)
				.expectFusion(Fuseable.ANY)
				.then(() -> processor.onNext(1))
				.thenRequest(1)
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void verifyDrainOnRequestInCaseOfFusion2() {
		ArrayList<Long> requests = new ArrayList<>();
		UnicastProcessor<Integer> processor = UnicastProcessor.create();
		StepVerifier.create(processor.doOnRequest(requests::add), 0)
				.expectFusion(Fuseable.ANY)
				.then(() -> {
					processor.onNext(1);
					processor.onComplete();
				})
				.thenRequest(1)
				.thenRequest(1)
				.thenRequest(1)
				.expectNext(1)
				.verifyComplete();

		assertThat(requests).containsExactly(1L, 1L, 1L);
	}
}
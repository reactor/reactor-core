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

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Arjen Poutsma
 * @author Sebastien Deleuze
 * @author Stephane Maldini
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

	@Test(expected = AssertionError.class)
	public void expectInvalidNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNext("baz")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void expectInvalidNexts() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo", "baz")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void expectInvalidNextMatches() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNextMatches("foo"::equals)
		            .expectNextMatches("baz"::equals)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void consumeNextWith() throws Exception {
		Flux<String> flux = Flux.just("bar");

		StepVerifier subscriber = StepVerifier.create(flux)
		                                      .consumeNextWith(s -> {
			                              if (!"foo".equals(s)) {
				                              throw new AssertionError("e:" + s);
			                              }
		                              })
		                                      .expectComplete();

		try {
			subscriber.verify();
		}
		catch (AssertionError error) {
			assertEquals("e:bar", error.getMessage());
		}
	}

	@Test
	public void consumeNextWith2() throws Exception {
		Flux<String> flux = Flux.just("bar");

		StepVerifier subscriber = StepVerifier.create(flux)
		                                      .consumeNextWith(s -> {
			                              if (!"foo".equals(s)) {
				                              throw new AssertionError(s);
			                              }
		                              })
		                                      .expectComplete();

		try {
			subscriber.verify();
		}
		catch (AssertionError error) {
			assertEquals("bar", error.getMessage());
		}
	}

	@Test(expected = AssertionError.class)
	public void missingNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void missingNextAsync() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.parallel());

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void expectNextCountLotsError() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		StepVerifier.create(flux, 0)
		            .thenRequest(100_000)
		            .expectNextCount(100_000)
		            .thenRequest(500_000)
		            .expectNextCount(499_999)
		            .thenRequest(500_000)
		            .expectNextCount(400_000)
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void expectNextCountError() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .expectNextCount(4)
		            .thenCancel()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void errorMatchesInvalid() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectErrorMatches(t -> t instanceof IllegalStateException)
		            .verify();
	}

	@Test
	public void consumeErrorWith() {
		Flux<String> flux = Flux.just("foo")
		                        .concatWith(Mono.error(new IllegalArgumentException()));

		try {
			StepVerifier.create(flux)
			            .expectNext("foo")
			            .consumeErrorWith(throwable -> {
				        if (!(throwable instanceof IllegalStateException)) {
					        throw new AssertionError(throwable.getClass()
					                                          .getSimpleName());
				        }
			        })
			            .verify();
		}
		catch (AssertionError error) {
			assertEquals("IllegalArgumentException", error.getMessage());
		}
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

	@Test(expected = AssertionError.class)
	public void cancelInvalid() {
		Flux<String> flux = Flux.just("bar", "baz");

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .thenCancel()
		            .verify();
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

		Assert.assertTrue(duration.toMillis() > 2 * interval);
	}

	@Test(expected = AssertionError.class)
	public void verifyDurationTimeout() {
		Flux<String> flux = Flux.interval(Duration.ofMillis(200))
		                        .map(l -> "foo")
		                        .take(2);

		StepVerifier.create(flux)
		            .expectNext("foo")
		            .expectNext("foo")
		            .expectComplete()
		            .verify(Duration.ofMillis(300));
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

	@Test(expected = AssertionError.class)
	public void verifyNextAsError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar"))
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifyNextAsError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .expectNextSequence(Arrays.asList("foo", "bar", "foobar", "bar"))
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void verifyRecordMatchesError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .recordWith(ArrayList::new)
		            .expectNextCount(3)
		            .expectRecordedMatches(c -> c.contains("foofoo"))
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifyRecordNullError() {
		Flux<String> flux = Flux.just("foo", "bar");

		StepVerifier.create(flux)
		            .recordWith(() -> null)
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifyRecordMatchesError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		StepVerifier.create(flux)
		            .expectNext("foo", "bar", "foobar")
		            .expectRecordedMatches(c -> c.size() == 3)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyRecordWith2() {
		final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux<Integer> flux = Flux.fromStream(source.stream());

		StepVerifier.create(flux)
		            .recordWith(ArrayList::new)
		            .expectNextCount(10)
		            .consumeRecordedWith(c -> Assert.assertTrue(c.containsAll(source)))
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifySubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectSubscriptionMatches(s -> false)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyConsumeSubscription() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .consumeSubscriptionWith(s -> Assert.assertTrue(s instanceof Fuseable.QueueSubscription))
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifyConsumeSubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .consumeSubscriptionWith(s -> Assert.fail())
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void verifyFusionError() {
		Mono.just("foo")
		    .hide()
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("foo")
		    .expectComplete()
		    .verify();
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

	@Test(expected = AssertionError.class)
	public void verifyNoFusionError() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
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

	@Test(expected = AssertionError.class)
	public void verifyFusionModeExpectedError() {
		Mono<String> flux = Mono.just("foo");

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.SYNC, Fuseable.ASYNC)
		            .expectNext("foo")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void verifyFusionModeExpected2Error() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.immediate());

		StepVerifier.create(flux)
		            .expectFusion(Fuseable.ASYNC, Fuseable.SYNC)
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verify();
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

	@Test(expected = AssertionError.class)
	public void verifyVirtualTimeNoEventError() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delaySubscription(Duration.ofDays(2)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofDays(2))
		            .expectNext("foo")
		            .expectNoEvent(Duration.ofDays(2))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeNoEventInterval() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3)).take(2))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3))
		            .expectNext(0L)
		            .expectNoEvent(Duration.ofSeconds(3))
		            .expectNext(1L)
		            .expectComplete()
		            .verify();
	}
	@Test(expected = AssertionError.class)
	public void verifyVirtualTimeNoEventIntervalError() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(3)).take(2))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3))
		            .expectNext(0L)
		            .expectNoEvent(Duration.ofSeconds(4))
		            .expectNext(1L)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void verifyVirtualTimeNoEventNever() {
		StepVerifier.withVirtualTime(() -> Mono.never().log())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofDays(10000))
		            .thenCancel()
		            .verify();
	}

	@Test(expected = AssertionError.class)
	public void verifyVirtualTimeNoEventNeverError() {
		StepVerifier.withVirtualTime(() -> Mono.never().log())
		            .expectNoEvent(Duration.ofDays(10000))
		            .thenCancel()
		            .verify();
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

		Assert.assertTrue(r.minus(Duration.ofSeconds(9))
		                   .isNegative());
	}

	@Test(expected = NullPointerException.class)
	public void verifyVirtualTimeNoLookupFails() {
		StepVerifier.withVirtualTime(1, Flux::empty, null);
	}

	@Test(expected = NullPointerException.class)
	public void verifyVirtualTimeNoScenarioFails() {
		StepVerifier.withVirtualTime(1, null);
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
		StepVerifier.withVirtualTime(0, () -> Flux.interval(Duration.ofSeconds(3))
		                                          .map(d -> "t" + d))
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
		StepVerifier.withVirtualTime(0,
				() -> Flux.just(123)
				          .subscribeOn(vts),
				() -> vts)
		            .thenRequest(1)
		            .thenAwait()
		            .expectNext(123)
		            .expectComplete()
		            .verify();

	}

	@Test(timeout = 1000)
	public void verifyCreatedSchedulerUsesVirtualTime() {
		//a timeout will occur if virtual time isn't used
		StepVerifier.withVirtualTime(0, () -> Flux.interval(Duration.ofSeconds(3)).map(d -> "t" + d),
				VirtualTimeScheduler::create)
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
		StepVerifier.withVirtualTime(0, () -> Flux.interval(Duration.ofSeconds(3)).map(d -> "t" + d),
				VirtualTimeScheduler::createForAll)
		        .thenRequest(1)
		        .thenAwait(Duration.ofSeconds(1))
		        .thenAwait(Duration.ofSeconds(2))
		        .expectNext("t0")
		        .thenCancel()
		        .verify();
	}

	@Test
	public void noSignalRealTime() {
		Duration verifyDuration = StepVerifier
				.create(Mono.never())
				.expectSubscription()
				.expectNoEvent(Duration.ofSeconds(1))
				.thenCancel()
				.verify(Duration.ofMillis(1100));

		assertThat(verifyDuration.toMillis(), is(greaterThanOrEqualTo(1000L)));
	}

	@Test(timeout = 500)
	public void noSignalVirtualTime() {
		StepVerifier
				.withVirtualTime(1, Mono::never)
				.expectSubscription()
				.expectNoEvent(Duration.ofSeconds(100))
				.thenCancel()
				.verify();
	}

	@Test
	public void longDelayAndNoTermination() {
		StepVerifier
				.withVirtualTime(Long.MAX_VALUE,
						() -> Flux.just("foo", "bar")
						          .delay(Duration.ofSeconds(5))
						          .concatWith(Mono.never())
				)
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
		Duration verifyDuration =
				StepVerifier.create(Flux.just("foo", "bar")
				            .delay(Duration.ofMillis(500)))
				            .expectSubscription()
				            .thenAwait(Duration.ofMillis(500))
				            .expectNext("foo")
				            .thenAwait(Duration.ofMillis(200))
				            .thenCancel()
				            .verify(Duration.ofMillis(1000));

		assertThat(verifyDuration.toMillis(), is(greaterThanOrEqualTo(700L)));
	}
}

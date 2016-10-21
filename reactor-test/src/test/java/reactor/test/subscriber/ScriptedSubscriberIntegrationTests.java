/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.subscriber;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;

/**
 * @author Arjen Poutsma
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class ScriptedSubscriberIntegrationTests {

	@Test
	public void expectNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectNext("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectNext("baz")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectNextAsync() {
		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectNext("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectNexts() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNext("foo", "bar")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidNexts() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNext("foo", "baz")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectNextWith() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNextWith("foo"::equals)
				.expectNextWith("bar"::equals)
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidNextWith() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNextWith("foo"::equals)
				.expectNextWith("baz"::equals)
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void consumeNextWith() throws Exception {
		Flux<String> flux = Flux.just("bar");

		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.consumeNextWith(s -> {
					if (!"foo".equals(s)) {
						throw new AssertionError("e:"+s);
					}
				})
				.expectComplete();

		try {
			subscriber.verify(flux);
		}
		catch (AssertionError error) {
			assertEquals("e:bar", error.getMessage());
		}
	}

	@Test
	public void consumeNextWith2() throws Exception {
		Flux<String> flux = Flux.just("bar");

		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.consumeNextWith(s -> {
					if (!"foo".equals(s)) {
						throw new AssertionError(s);
					}
				})
				.expectComplete();

		try {
			subscriber.verify(flux);
		}
		catch (AssertionError error) {
			assertEquals("bar", error.getMessage());
			error.printStackTrace();
		}
	}

	@Test(expected = AssertionError.class)
	public void missingNext() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void missingNextAsync() {
		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectNextCount() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create(0)
		                  .thenRequest(1)
		                  .expectNextCount(1)
		                  .thenRequest(1)
		                  .expectNextCount(1)
		                  .expectComplete()
		                  .verify(flux);
	}


	@Test
	public void expectNextCountLots() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		ScriptedSubscriber.create(0)
		                  .thenRequest(100_000)
		                  .expectNextCount(100_000)
		                  .thenRequest(500_000)
		                  .expectNextCount(500_000)
		                  .thenRequest(500_000)
		                  .expectNextCount(400_000)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectNextCountLotsError() {
		Flux<Integer> flux = Flux.range(0, 1_000_000);

		ScriptedSubscriber.create(0)
		                  .thenRequest(100_000)
		                  .expectNextCount(100_000)
		                  .thenRequest(500_000)
		                  .expectNextCount(499_999)
		                  .thenRequest(500_000)
		                  .expectNextCount(400_000)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void expectNextCount2() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
		                  .expectNext("foo", "bar")
		                  .expectNextCount(2)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void expectNextCount3() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
		                  .expectNext("foo")
		                  .expectNextCount(1)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void expectNextCountZero() {
		Flux<String> flux = Flux.empty();

		ScriptedSubscriber.create()
		                  .expectNextCount(0)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectNextCountError() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
		                  .expectNextCount(4)
		                  .thenCancel()
		                  .verify(flux);
	}

	@Test
	public void error() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectError()
				.verify(flux);
	}

	@Test
	public void errorClass() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectError(IllegalArgumentException.class)
				.verify(flux);
	}

	@Test
	public void errorMessage() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new
				IllegalArgumentException("Error message")));

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectErrorMessage("Error message")
				.verify(flux);
	}

	@Test
	public void errorWith() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectErrorWith(t -> t instanceof IllegalArgumentException)
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void errorWithInvalid() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectErrorWith(t -> t instanceof IllegalStateException)
				.verify(flux);
	}

	@Test
	public void consumeErrorWith() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		try {
			ScriptedSubscriber.create()
					.expectNext("foo")
					.consumeErrorWith(throwable -> {
						if (!(throwable instanceof IllegalStateException)) {
							throw new AssertionError(throwable.getClass().getSimpleName());
						}
					})
					.verify(flux);
		}
		catch (AssertionError error) {
			assertEquals("IllegalArgumentException", error.getMessage());
		}
	}

	@Test
	public void request() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create(1)
				.thenRequest(1)
				.expectNext("foo")
				.thenRequest(1)
				.expectNext("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void cancel() {
		Flux<String> flux = Flux.just("foo", "bar", "baz");

		ScriptedSubscriber.create()
				.expectNext("foo")
				.thenCancel()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void cancelInvalid() {
		Flux<String> flux = Flux.just("bar", "baz");

		ScriptedSubscriber.create()
				.expectNext("foo")
				.thenCancel()
				.verify(flux);
	}

	@Test(expected = IllegalStateException.class)
	public void notSubscribed() {
		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectComplete()
				.verify(Duration.ofMillis(100));
	}

	@Test(expected = IllegalStateException.class)
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber<String> s =
				ScriptedSubscriber.<String>create().expectNext("foo")
				                                   .expectNext("bar")
				                                   .expectComplete();

		s.verify(flux);
		s.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void subscribedTwice2() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber<String> s =
				ScriptedSubscriber.<String>create().expectNext("foo", "bar")
				                                   .expectComplete();

		flux.subscribe(s);
		flux.subscribe(s);
		s.verify();
	}

	@Test
	public void verifyVirtualTimeOnSubscribe() {
		ScriptedSubscriber.enableVirtualTime();
		Mono<String> mono = Mono.delay(Duration.ofDays(2))
		                        .map(l -> "foo");

		ScriptedSubscriber.create()
		                  .advanceTimeBy(Duration.ofDays(3))
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(mono);

	}

	@Test
	public void verifyVirtualTimeOnError() {
		ScriptedSubscriber.enableVirtualTime();
		Mono<String> mono = Mono.never()
		                        .timeout(Duration.ofDays(2))
		                        .map(l -> "foo");

		ScriptedSubscriber.create()
		                  .advanceTimeTo(Instant.now().plus(Duration.ofDays(2)))
		                  .expectError(TimeoutException.class)
		                  .verify(mono);

	}

	@Test
	public void verifyVirtualTimeOnNext() {
		ScriptedSubscriber.enableVirtualTime();
		Flux<String> flux = Flux.just("foo", "bar", "foobar")
		                        .delay(Duration.ofHours(1))
		                        .log();

		ScriptedSubscriber.create()
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectNext("foo")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectNext("bar")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectNext("foobar")
		                  .expectComplete()
		                  .verify(flux);

	}

	@Test
	public void verifyVirtualTimeOnComplete() {
		ScriptedSubscriber.enableVirtualTime();
		Flux<?> flux = Flux.empty()
		                   .delaySubscription(Duration.ofHours(1))
		                   .log();

		ScriptedSubscriber.create()
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectComplete()
		                  .verify(flux);

	}

	@Test
	public void verifyVirtualTimeOnNextInterval() {
		ScriptedSubscriber.enableVirtualTime();
		Flux<String> flux = Flux.interval(Duration.ofSeconds(3))
		                        .map(d -> "t" + d);

		ScriptedSubscriber.create()
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectNext("t0")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectNext("t1")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectNext("t2")
		                  .thenCancel()
		                  .verify(flux);

	}

	@Test
	public void verifyThenOnCompleteInterval() {
		DirectProcessor<Void> p = DirectProcessor.create();

		Flux<String> flux = Flux.range(0, 3)
		                        .map(d -> "t" + d)
								.takeUntilOther(p);

		ScriptedSubscriber.create(2)
		                  .expectNext("t0", "t1")
		                  .then(p::onComplete)
		                  .expectComplete()
		                  .verify(flux);

	}

	@Test
	public void verifyVirtualTimeOnErrorInterval() {
		ScriptedSubscriber.enableVirtualTime();
		Flux<String> flux = Flux.interval(Duration.ofSeconds(3))
		                        .map(d -> "t" + d);

		ScriptedSubscriber.create(0)
		                  .thenRequest(1)
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectNext("t0")
		                  .thenRequest(1)
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectNext("t1")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectError(IllegalStateException.class)
		                  .verify(flux);

	}

	@Test
	public void verifyDuration() {
		long interval = 200;
		Flux<String> flux = Flux.interval(Duration.ofMillis(interval))
		                        .map(l -> "foo")
		                        .take(2);

		Duration duration = ScriptedSubscriber.create()
		                                      .expectNext("foo")
		                                      .expectNext("foo")
		                                      .expectComplete()
		                                      .verify(flux, Duration.ofMillis(500));

		Assert.assertTrue(duration.toMillis() > 2*interval);
	}

	@Test(expected = AssertionError.class)
	public void verifyDurationTimeout() {
		Flux<String> flux = Flux.interval(Duration.ofMillis(200)).map(l -> "foo" ).take(2);

		ScriptedSubscriber.create()
				.expectNext("foo")
				.expectNext("foo")
				.expectComplete()
				.verify(flux, Duration.ofMillis(300));
	}

	@Test
	public void verifyNever() {
		Flux<String> flux = Flux.never();

		ScriptedSubscriber.create()
		                  .expectSubscription()
		                  .thenCancel()
		                  .verify(flux);
	}

	@Test
	public void verifySubscription() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectSubscriptionWith(s -> s instanceof Fuseable.QueueSubscription)
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyNextAs() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .expectNextAs(Arrays.asList("foo", "bar", "foobar"))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyNextAsError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .expectNextAs(Arrays.asList("foo", "bar"))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyNextAsError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .expectNextAs(Arrays.asList("foo", "bar", "foobar", "bar"))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyNextAs2() {
		final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux<Integer> flux = Flux.fromStream(source.stream());

		ScriptedSubscriber.create()
		                  .expectNextAs(source)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyRecordWith() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .recordWith(ArrayList::new)
		                  .expectNextCount(3)
		                  .expectRecordedWith(c -> c.contains("foobar"))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyRecordWithError() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .recordWith(ArrayList::new)
		                  .expectNextCount(3)
		                  .expectRecordedWith(c -> c.contains("foofoo"))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyRecordNullError() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
		                  .recordWith(() -> null)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyRecordWithError2() {
		Flux<String> flux = Flux.just("foo", "bar", "foobar");

		ScriptedSubscriber.create()
		                  .expectNext("foo", "bar", "foobar")
		                  .expectRecordedWith(c -> c.size() == 3)
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyRecordWith2() {
		final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux<Integer> flux = Flux.fromStream(source.stream());

		ScriptedSubscriber.create()
		                  .recordWith(ArrayList::new)
		                  .expectNextCount(10)
		                  .consumeRecordedWith(c -> Assert.assertTrue(c.containsAll(source)))
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifySubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectSubscriptionWith(s -> false)
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyConsumeSubscription() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .consumeSubscriptionWith(s -> Assert.assertTrue(s instanceof Fuseable.QueueSubscription))
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyConsumeSubscriptionError() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .consumeSubscriptionWith(s -> Assert.fail())
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyFusion() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectFusion()
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyFusionError() {
		Mono<String> flux = Mono.just("foo")
		                        .hide();

		ScriptedSubscriber.create()
		                  .expectFusion()
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyFusionModeRequest() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectFusion(Fuseable.SYNC)
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyFusionModeExpected() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectFusion(Fuseable.SYNC, Fuseable.SYNC)
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyFusionModeExpectedError() {
		Mono<String> flux = Mono.just("foo");

		ScriptedSubscriber.create()
		                  .expectFusion(Fuseable.SYNC, Fuseable.ASYNC)
		                  .expectNext("foo")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test
	public void verifyFusionModeExpected2() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.immediate());

		ScriptedSubscriber.create()
		                  .expectFusion(Fuseable.SYNC | Fuseable.ASYNC, Fuseable.ASYNC)
		                  .expectNext("foo", "bar")
		                  .expectComplete()
		                  .verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void verifyFusionModeExpected2Error() {
		Flux<String> flux = Flux.just("foo", "bar")
		                        .publishOn(Schedulers.immediate());

		ScriptedSubscriber.create()
		                  .expectFusion(Fuseable.ASYNC, Fuseable.SYNC)
		                  .expectNext("foo", "bar")
		                  .expectComplete()
		                  .verify(flux);
	}


	@After
	public void cleanup(){
		ScriptedSubscriber.disableVirtualTime();
	}
}
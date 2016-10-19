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
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;
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
	public void expectValue() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValue() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("baz")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectValueAsync() {
		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectValues() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValues("foo", "bar")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValues() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValues("foo", "baz")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectValueWith() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValueWith("foo"::equals)
				.expectValueWith("bar"::equals)
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValueWith() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValueWith("foo"::equals)
				.expectValueWith("baz"::equals)
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void consumeValueWith() throws Exception {
		Flux<String> flux = Flux.just("bar");

		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.consumeValueWith(s -> {
					if (!"foo".equals(s)) {
						throw new AssertionError(s);
					}
				})
				.expectComplete();

		try {
			subscriber.verify(flux);
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - bar", error.getMessage());
		}
	}

	@Test(expected = AssertionError.class)
	public void missingValue() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void missingValueAsync() {
		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void expectValueCount() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.expectValueCount(2)
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void error() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectError()
				.verify(flux);
	}

	@Test
	public void errorClass() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectError(IllegalArgumentException.class)
				.verify(flux);
	}

	@Test
	public void errorMessage() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new
				IllegalArgumentException("Error message")));

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorMessage("Error message")
				.verify(flux);
	}

	@Test
	public void errorWith() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalArgumentException)
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void errorWithInvalid() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalStateException)
				.verify(flux);
	}

	@Test
	public void consumeErrorWith() {
		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));

		try {
			ScriptedSubscriber.create()
					.expectValue("foo")
					.consumeErrorWith(throwable -> {
						if (!(throwable instanceof IllegalStateException)) {
							throw new AssertionError(throwable.getClass().getSimpleName());
						}
					})
					.verify(flux);
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - IllegalArgumentException", error.getMessage());
		}
	}

	@Test
	public void request() {
		Flux<String> flux = Flux.just("foo", "bar");

		ScriptedSubscriber.create(1)
				.doRequest(1)
				.expectValue("foo")
				.doRequest(1)
				.expectValue("bar")
				.expectComplete()
				.verify(flux);
	}

	@Test
	public void cancel() {
		Flux<String> flux = Flux.just("foo", "bar", "baz");

		ScriptedSubscriber.create()
				.expectValue("foo")
				.doCancel()
				.verify(flux);
	}

	@Test(expected = AssertionError.class)
	public void cancelInvalid() {
		Flux<String> flux = Flux.just("bar", "baz");

		ScriptedSubscriber.create()
				.expectValue("foo")
				.doCancel()
				.verify(flux);
	}

	@Test(expected = IllegalStateException.class)
	public void notSubscribed() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(Duration.ofMillis(100));
	}

	@Test
	public void verifyDuration() {
		Flux<String> flux = Flux.interval(Duration.ofMillis(200)).map(l -> "foo").take(2);

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete()
				.verify(flux, Duration.ofMillis(500));
	}

	@Test
	public void verifyVirtualTimeOnSubscribe() {
		ScriptedSubscriber.enableVirtualTime();
		Mono<String> mono = Mono.delay(Duration.ofDays(2))
		                        .map(l -> "foo");

		ScriptedSubscriber.create()
		                  .advanceTimeBy(Duration.ofDays(3))
		                  .expectValue("foo")
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
		                  .expectValue("foo")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectValue("bar")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectValue("foobar")
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
		                  .expectValue("t0")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t1")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t2")
		                  .doCancel()
		                  .verify(flux);

	}

	@Test
	public void verifyThenOnCompleteInterval() {
		DirectProcessor<Void> p = DirectProcessor.create();

		Flux<String> flux = Flux.range(0, 3)
		                        .map(d -> "t" + d)
								.takeUntilOther(p);

		ScriptedSubscriber.create(2)
		                  .expectValues("t0", "t1")
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
		                  .doRequest(1)
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t0")
		                  .doRequest(1)
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t1")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectError(IllegalStateException.class)
		                  .verify(flux);

	}

	@Test(expected = AssertionError.class)
	public void verifyDurationTimeout() {
		Flux<String> flux = Flux.interval(Duration.ofMillis(200)).map(l -> "foo" ).take(2);

		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete()
				.verify(flux, Duration.ofMillis(300));
	}

	@After
	public void cleanup(){
		ScriptedSubscriber.disableVirtualTime();
	}
}
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

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;

/**
 * @author Arjen Poutsma
 */
public class ScriptedSubscriberIntegrationTests {

	@Test
	public void expectValue() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValue() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectValue("baz")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void expectValueAsync() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void expectValues() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValues("foo", "bar")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValues() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValues("foo", "baz")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void expectValueWith() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValueWith("foo"::equals)
				.expectValueWith("bar"::equals)
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValueWith() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValueWith("foo"::equals)
				.expectValueWith("baz"::equals)
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void consumeValueWith() throws Exception {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.consumeValueWith(s -> {
					if (!"foo".equals(s)) {
						throw new AssertionError(s);
					}
				})
				.expectComplete();

		Flux<String> flux = Flux.just("bar");
		flux.subscribe(subscriber);

		try {
			subscriber.verify();
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - bar", error.getMessage());
		}
	}

	@Test(expected = AssertionError.class)
	public void missingValue() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void missingValueAsync() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar").publishOn(Schedulers.parallel());
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void expectValueCount() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>expectValueCount(2)
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void error() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectError();

		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void errorClass() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectError(IllegalArgumentException.class);

		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void errorWith() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalArgumentException);

		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void errorWithInvalid() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalStateException);

		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void consumeErrorWith() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.consumeErrorWith(throwable -> {
					if (!(throwable instanceof IllegalStateException)) {
						throw new AssertionError(throwable.getClass().getSimpleName());
					}
				});

		Flux<String> flux = Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException()));
		flux.subscribe(subscriber);

		try {
			subscriber.verify();
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - IllegalArgumentException", error.getMessage());
		}
	}

	@Test
	public void request() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create(1)
				.doRequest(1)
				.expectValue("foo")
				.doRequest(1)
				.expectValue("bar")
				.expectComplete();

		Flux<String> flux = Flux.just("foo", "bar");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test
	public void cancel() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.doCancel();

		Flux<String> flux = Flux.just("foo", "bar", "baz");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = AssertionError.class)
	public void cancelInvalid() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.doCancel();

		Flux<String> flux = Flux.just("bar", "baz");
		flux.subscribe(subscriber);

		subscriber.verify();
	}

	@Test(expected = IllegalStateException.class)
	public void notSubscribed() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectComplete();

		subscriber.verify();
	}

	@Test
	public void verifyDuration() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete();

		Flux<String> flux = Flux.interval(Duration.ofMillis(200)).map(l -> "foo").take(2);
		flux.subscribe(subscriber);

		subscriber.verify(Duration.ofMillis(500));
	}

	@Test(expected = AssertionError.class)
	public void verifyDurationTimeout() {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete();

		Flux<String> flux = Flux.interval(Duration.ofMillis(200)).map(l -> "foo" ).take(2);
		flux.subscribe(subscriber);

		subscriber.verify(Duration.ofMillis(300));
	}

}
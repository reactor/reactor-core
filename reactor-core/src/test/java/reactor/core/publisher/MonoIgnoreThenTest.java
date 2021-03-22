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

import java.time.Duration;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

class MonoIgnoreThenTest {

	@Nested
	class ThenIgnoreVariant {

		Publisher<Void> scenario(){
			return Mono.just(1)
			           .thenEmpty(Mono.delay(Duration.ofSeconds(123)).then());
		}

		@Test
		void justThenIgnore() {
			StepVerifier.create(Mono.just(1)
			                        .then())
			            .verifyComplete();
		}

		@Test
		void justThenEmptyThenIgnoreWithTime() {
			StepVerifier.withVirtualTime(this::scenario)
			            .thenAwait(Duration.ofSeconds(123))
			            .verifyComplete();
		}

		@Test
		void justThenIgnoreWithCancel() {
			TestPublisher<String> cancelTester = TestPublisher.create();

			Disposable disposable = cancelTester
					.flux()
					.then()
					.subscribe();

			disposable.dispose();

			cancelTester.assertCancelled();
		}
	}

	@Test
		// https://github.com/reactor/reactor-core/issues/2561
	void raceTest2561() {
		final Scheduler scheduler = Schedulers.newSingle("non-test-thread");
		final Mono<String> getCurrentThreadName =
				Mono.fromSupplier(() -> Thread.currentThread().getName());
		for (int i = 0; i < 100000; i++) {
			StepVerifier.create(getCurrentThreadName.publishOn(scheduler)
			                                        .then(getCurrentThreadName)
			                                        .then(getCurrentThreadName)
			                                        .then(getCurrentThreadName))
			            .assertNext(threadName -> {
				            assertThat(threadName).startsWith("non-test-thread");
			            })
			            .verifyComplete();
		}
	}

	@Test
	void justThenEmpty() {
		StepVerifier.create(Mono.just(1)
		                        .thenEmpty(Flux.empty()))
		            .verifyComplete();
	}

	@Test
	void justThenThen() {
		StepVerifier.create(Mono.just(0)
		                        .then(Mono.just(1))
		                        .then(Mono.just(2)))
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
	void justThenReturn() {
		StepVerifier.create(Mono.just(0).thenReturn(2))
		            .expectNext(2)
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	void fluxThenMonoAndShift() {
		StepVerifier.create(Flux.just("Good Morning", "Hello")
		                        .then(Mono.just("Good Afternoon"))
		                        .then(Mono.just("Bye")))
		            .expectNext("Bye")
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	void monoThenMonoAndShift() {
		StepVerifier.create(Mono.just("Good Morning")
		                        .then(Mono.just("Good Afternoon"))
		                        .then(Mono.just("Bye")))
		            .expectNext("Bye")
		            .verifyComplete();
	}

	@Test
	void scanOperator() {
		MonoIgnoreThen<String> test = new MonoIgnoreThen<>(new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanThenIgnoreMain() {
		AssertSubscriber<String> actual = new AssertSubscriber<>();
		MonoIgnoreThen.ThenIgnoreMain<String> test = new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

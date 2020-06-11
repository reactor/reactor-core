/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Simon Baslé
 */
class SinksTest {

	@Nested
	class FluxSinks {

		@Test
		void coldAllowsSeveralSubscribers() {
			final SinkFlux.Standalone<String> standaloneFluxSink = Sinks.coldFlux();
			standaloneFluxSink.next("A")
			                  .next("B")
			                  .complete();
			Flux<String> flux = standaloneFluxSink.asFlux();

			StepVerifier.create(flux, StepVerifierOptions.create().scenarioName("first subscription"))
			            .expectNext("A", "B")
			            .verifyComplete();

			StepVerifier.create(flux, StepVerifierOptions.create().scenarioName("second subscription"))
			            .expectNext("A", "B")
			            .verifyComplete();
		}

		@Test
		void hotIsMulticast() throws InterruptedException {
			final SinkFlux.Standalone<Integer> standaloneFluxSink = Sinks.hotFlux();
			Flux<Integer> flux = standaloneFluxSink.asFlux();

			CountDownLatch latch = new CountDownLatch(2);
			ExecutorService executorService = Executors.newFixedThreadPool(2);
			Future<Duration> f1 = executorService.submit(() ->
					StepVerifier.create(flux,
							StepVerifierOptions.create().scenarioName("first subscription"))
					            .expectSubscription()
					            .then(latch::countDown)
					            .expectNext(1, 2)
					            .verifyComplete()
			);
			Future<Duration> f2 = executorService.submit(() ->
					StepVerifier.create(flux,
							StepVerifierOptions.create().scenarioName("first subscription"))
					            .expectSubscription()
					            .then(latch::countDown)
					            .expectNext(1, 2)
					            .verifyComplete()
			);

			//give subscribers a bit of time to arrive, lest elements be dropped
			assertThat(latch.await(1, TimeUnit.SECONDS)).as("subscribers ready").isTrue();

			//feed the hot sink
			standaloneFluxSink.next(1).next(2).complete();

			assertThatCode(f1::get).doesNotThrowAnyException();
			assertThatCode(f2::get).doesNotThrowAnyException();
		}

		@Test
		void coldAccumulatesBeforeSubscriber() {
			SinkFlux.Standalone<Integer> standaloneFluxSink = Sinks.coldFlux();
			standaloneFluxSink.next(1)
			         .next(2)
			         .next(3)
			         .complete();

			StepVerifier.create(standaloneFluxSink.asFlux())
			            .expectNext(1, 2, 3)
			            .verifyComplete();
		}

		@Test
		void hotDropsBeforeSubscriber() {
			SinkFlux.Standalone<Integer> standaloneFluxSink = Sinks.hotFlux();
			standaloneFluxSink.next(1)
			         .next(2)
			         .next(3);

			StepVerifier.create(standaloneFluxSink.asFlux())
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(100))
			            .then(() -> standaloneFluxSink.next(4).next(5))
			            .expectNext(4, 5)
			            .then(standaloneFluxSink::complete)
			            .verifyComplete();
		}

		@Test
		void coldHonorsSubscriberBackpressure() {
			SinkFlux.Standalone<Integer> standaloneFluxSink = Sinks.coldFlux();

			StepVerifier.create(standaloneFluxSink.asFlux(), 0)
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(100))
			            .then(() -> standaloneFluxSink.next(1)
			                                 .next(2)
			                                 .next(3)
			                                 .complete())
			            .expectNoEvent(Duration.ofMillis(100))
			            .thenRequest(10)
			            .expectNext(1, 2, 3)
			            .verifyComplete();
		}

		@Test
		void hotHonorsSubscriberBackpressure() {
			SinkFlux.Standalone<Integer> standaloneFluxSink = Sinks.hotFlux();

			StepVerifier.create(standaloneFluxSink.asFlux(), 0)
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(100))
			            .then(() -> standaloneFluxSink.next(1)
			                                 .next(2)
			                                 .next(3)
			                                 .complete())
			            .expectNoEvent(Duration.ofMillis(100))
			            .thenRequest(10)
			            .expectNext(1, 2, 3)
			            .verifyComplete();
		}

	}

}
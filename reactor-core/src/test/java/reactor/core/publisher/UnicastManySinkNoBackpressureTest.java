/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.Test;

import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.Emission;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class UnicastManySinkNoBackpressureTest {

	@Test
	void noSubscribers() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitNext("hi")).isEqualTo(Emission.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noSubscribersTryError() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitError(new NullPointerException())).isEqualTo(Emission.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noSubscribersTryComplete() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitComplete()).isEqualTo(Emission.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noRequest() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 0)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).isEqualTo(Emission.FAIL_OVERFLOW);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	void singleRequest() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 1)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).as("requested").isEqualTo(Emission.OK);
		            })
		            .expectNextCount(1)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).as("overflow").isEqualTo(Emission.FAIL_OVERFLOW);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	void cancelled() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 0).thenCancel().verify();

		assertThat(sink.tryEmitNext("hi")).isEqualTo(Emission.FAIL_CANCELLED);
	}

	@Test
	void beforeSubscriberEmitNextIsIgnoredKeepsSinkOpen() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		sink.emitNext("hi");

		StepVerifier.create(sink.asFlux())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> {
		            	sink.emitNext("second");
		            	sink.emitComplete();
		            })
		            .expectNext("second")
		            .verifyComplete();
	}

}
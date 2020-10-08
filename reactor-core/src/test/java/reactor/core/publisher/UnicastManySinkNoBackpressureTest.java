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

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class UnicastManySinkNoBackpressureTest {

	@Test
	void currentSubscriberCount() {
		Sinks.Many<Integer> sink = UnicastManySinkNoBackpressure.create();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();
	}

	@Test
	void noSubscribers() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitNext("hi")).isEqualTo(EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noSubscribersTryError() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitError(new NullPointerException())).isEqualTo(EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noSubscribersTryComplete() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		assertThat(sink.tryEmitComplete()).isEqualTo(EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void noRequest() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 0)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).isEqualTo(Sinks.EmitResult.FAIL_OVERFLOW);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	void singleRequest() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 1)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).as("requested").isEqualTo(Sinks.EmitResult.OK);
		            })
		            .expectNextCount(1)
		            .then(() -> {
			            assertThat(sink.tryEmitNext("hi")).as("overflow").isEqualTo(
					            EmitResult.FAIL_OVERFLOW);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	void cancelled() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		StepVerifier.create(sink.asFlux(), 0).thenCancel().verify();

		assertThat(sink.tryEmitNext("hi")).isEqualTo(Sinks.EmitResult.FAIL_CANCELLED);
	}

	@Test
	void completed() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		sink.asFlux().subscribe();
		sink.tryEmitComplete().orThrow();

		assertThat(sink.tryEmitNext("hi")).isEqualTo(EmitResult.FAIL_TERMINATED);
	}

	@Test
	void errored() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();
		sink.asFlux().subscribe(v -> {}, e -> {});
		sink.tryEmitError(new IllegalArgumentException("boom")).orThrow();

		assertThat(sink.tryEmitNext("hi")).isEqualTo(EmitResult.FAIL_TERMINATED);
	}

	@Test
	void beforeSubscriberEmitNextIsIgnoredKeepsSinkOpen() {
		Sinks.Many<Object> sink = UnicastManySinkNoBackpressure.create();

		sink.emitNext("hi", FAIL_FAST);

		StepVerifier.create(sink.asFlux())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> {
		            	sink.emitNext("second", FAIL_FAST);
		            	sink.emitComplete(FAIL_FAST);
		            })
		            .expectNext("second")
		            .verifyComplete();
	}

	@Test
	void scanTerminatedCancelled() {
		Sinks.Many<Integer> sink = UnicastManySinkNoBackpressure.create();
		sink.asFlux().subscribe();

		assertThat(sink.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sink.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sink.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		//this sink doesn't retain errors :(
//		assertThat(sink.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");

		assertThat(sink.scan(Scannable.Attr.CANCELLED)).as("pre-cancellation").isFalse();

		((UnicastManySinkNoBackpressure<?>) sink).cancel();

		assertThat(sink.scan(Scannable.Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	void inners() {
		Sinks.Many<Integer> sink1 = UnicastManySinkNoBackpressure.create();
		Sinks.Many<Integer> sink2 = UnicastManySinkNoBackpressure.create();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink1.inners()).as("before subscription notScannable").isEmpty();
		assertThat(sink2.inners()).as("before subscription notScannable").isEmpty();

		sink1.asFlux().subscribe(notScannable);
		sink2.asFlux().subscribe(scannable);

		assertThat(sink1.inners())
				.asList()
				.as("after notScannable subscription")
				.containsExactly(Scannable.from("NOT SCANNABLE"));

		assertThat(sink2.inners())
				.asList()
				.as("after scannable subscription")
				.containsExactly(scannable);
	}

}
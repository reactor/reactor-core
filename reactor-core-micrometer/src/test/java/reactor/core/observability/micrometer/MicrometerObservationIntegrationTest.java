/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.observability.micrometer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.junit.jupiter.api.Tag;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
@Tag("slow")
public class MicrometerObservationIntegrationTest extends SampleTestRunner {

	MicrometerObservationIntegrationTest() {
		super(SampleTestRunner.SampleRunnerConfig.builder()
			.build());
	}

	@Override
	public SampleTestRunnerConsumer yourCode() throws Exception {
		final Scheduler delayScheduler = Schedulers.newSingle("test");
		final IllegalStateException EXCEPTION = new IllegalStateException("expected error");
		return (bb, meterRegistry) -> {
			Span beforeStart = bb.getTracer().currentSpan();

			Function<Integer, Mono<String>> querySimulator = id ->
				Mono.delay(Duration.ofMillis(500), delayScheduler)
					.tag("endpoint", "simulated/" + id)
					.map(ignored -> "query for id " + id)
					.doOnNext(v -> {
						if (id == 2L) throw EXCEPTION;
					})
					.name("query" + id)
					.tap(Micrometer.observation(getObservationRegistry()));

			Flux.range(0, 100)
				.name("testFlux")
				.tag("interval", "500ms")
				.take(3)
				.tag("size", "3")
				.concatMap(querySimulator)
				.tap(Micrometer.observation(getObservationRegistry()))
				.onErrorReturn("ended with error") // prevent error throwing. the tap should still get notified
				.blockLast();

			SpansAssert spansAssert = SpansAssert.assertThat(bb.getFinishedSpans());
			SpansAssert.SpansAssertReturningAssert assertThatMain = spansAssert.assertThatASpanWithNameEqualTo("test-flux");
			SpansAssert.SpansAssertReturningAssert assertThatQuery2 = spansAssert.assertThatASpanWithNameEqualTo("query2");

			spansAssert.hasSize(4);

			assertThatMain
				.hasTag("reactor.status", "error")
				.hasTag("reactor.type", "Flux")
				.hasTag("interval", "500ms")
				.hasTag("size", "3")
				//TODO propose new duration assertion? span's timestamps should return Instant, not long. OTel is using nanos, Brave is storing long microsecond
//				.satisfies(span -> assertThat(Duration.ofNanos(span.getEndTimestamp() - span.getStartTimestamp()))
//					.as("duration")
//					.isGreaterThanOrEqualTo(Duration.ofMillis(1500))
//				)
				//OTEL doesn't really capture the exception type, only the message
				.thenThrowable().hasMessage(EXCEPTION.getMessage());

			//query2 span
			assertThatQuery2
				.hasTag("endpoint", "simulated/2")
				.thenThrowable().hasMessage(EXCEPTION.getMessage());

			//quick assert query0 and query1
			spansAssert
				.thenASpanWithNameEqualTo("query0")
				.doesNotHaveEventWithNameEqualTo("exception")
				.hasTag("endpoint", "simulated/0")
				.backToSpans()
				.hasASpanWithName("query1");

			assertThat(bb.getTracer().currentSpan())
				.as("no leftover span in main thread")
				.isNotSameAs(beforeStart) //something happened
				.isEqualTo(beforeStart); //original span was restored

			//finally, assert that the delay thread was not polluted either
			CountDownLatch latch = new CountDownLatch(1);
			delayScheduler.schedule(() -> {
				try {
					assertThat(bb.getTracer().currentSpan())
						.as("no leftover span in delay thread")
						.isNull();
				}
				finally {
					latch.countDown();
				}
			});
			latch.await(10, TimeUnit.SECONDS);
		};
	}
}

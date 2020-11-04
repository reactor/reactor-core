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
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class FluxTimedTest {

	private static Runnable emit(String value, Duration delay, Sinks.Many<String> sink, VirtualTimeScheduler vts) {
		return () -> {
			vts.advanceTimeBy(delay);
			sink.emitNext(value, FAIL_FAST);
		};
	}

	@Test
	void compareTimedElapsedNanosToElapsedT1() {
		//we'll manipulate the vts and emit values manually in order to show what happens with half milliseconds
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		Sinks.Many<String> sink = Sinks.unsafe().many().multicast().directBestEffort();

		Consumer<? super Timed<Tuple2<Long, String>>> timedSimilarToElapsed = v -> {
			assertThat(v.elapsed().toMillis())
					.as(v.toString())
					//Duration.toMillis() drops the nanosecond part entirely
					.isEqualTo(123L)
					.isBetween(v.get().getT1() - 1L, v.get().getT1());
		};

		StepVerifier.withVirtualTime(() -> sink.asFlux().elapsed().timed(),
				() -> vts, Long.MAX_VALUE)
		            .then(emit("A", Duration.ofNanos(123 * 1_000_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("B", Duration.ofNanos(1234 * 100_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("C", Duration.ofNanos(1235 * 100_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("D", Duration.ofNanos(12355 * 10_000), sink, vts)) //this is the one that gets truncated down
		            .assertNext(timedSimilarToElapsed)
		            .then(() -> sink.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	void compareTimedTimetampNanosToTimestampT1() {
		//we'll manipulate the vts and emit values manually in order to show what happens with half milliseconds
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		Sinks.Many<String> sink = Sinks.unsafe().many().multicast().directBestEffort();

		Consumer<? super Timed<Tuple2<Long, String>>> timedSimilarToTimestamp = v -> {
			assertThat(v.timestamp().toEpochMilli())
					.as(v.toString())
					.isLessThan(1000L)
					//Instant.toEpochMilli() drops the nanosecond part entirely
					.isBetween(v.get().getT1() -1L, v.get().getT1());
		};

		StepVerifier.withVirtualTime(() -> sink.asFlux().timestamp().timed(),
				() -> vts, Long.MAX_VALUE)
		            .then(emit("A", Duration.ofNanos(123 * 1_000_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("B", Duration.ofNanos(1234 * 100_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("C", Duration.ofNanos(1235 * 100_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("D", Duration.ofNanos(12355 * 10_000), sink, vts)) //this is the one that gets truncated down
		            .assertNext(timedSimilarToTimestamp)
		            .then(() -> sink.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

}
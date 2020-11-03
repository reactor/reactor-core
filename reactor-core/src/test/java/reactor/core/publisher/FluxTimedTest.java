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
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

class FluxTimedTest {

	@Test
	void compareTimedElapsedNanosToElapsedT1() {
		Consumer<? super Timed<Tuple2<Long, Integer>>> timedSimilarToElapsed = v -> {
			assertThat(v.elapsed().toMillis())
					.as(v.toString())
					//Duration.toMillis() drops the nanosecond part entirely
					.isBetween(v.get().getT1() - 1L, v.get().getT1());
		};

		Flux.range(1, 5)
		    .delayElements(Duration.ofMillis(123))
		    .elapsed()
		    .timed()
		    .as(StepVerifier::create)
		    .assertNext(timedSimilarToElapsed)
		    .assertNext(timedSimilarToElapsed)
		    .assertNext(timedSimilarToElapsed)
		    .assertNext(timedSimilarToElapsed)
		    .assertNext(timedSimilarToElapsed)
		    .verifyComplete();
	}

	@Test
	void compareTimedTimetampNanosToTimestampT1() {
		Consumer<? super Timed<Tuple2<Long, Integer>>> timedSimilarToTimestamp = v -> {
			assertThat(v.timestamp().toEpochMilli())
					.as(v.toString())
					//Instant.toEpochMilli() drops the nanosecond part entirely
					.isBetween(v.get().getT1() -1L, v.get().getT1());
		};

		Flux.range(1, 5)
		    .delayElements(Duration.ofMillis(123))
		    .timestamp()
		    .timed()
		    .as(StepVerifier::create)
		    .assertNext(timedSimilarToTimestamp)
		    .assertNext(timedSimilarToTimestamp)
		    .assertNext(timedSimilarToTimestamp)
		    .assertNext(timedSimilarToTimestamp)
		    .assertNext(timedSimilarToTimestamp)
		    .verifyComplete();
	}

}
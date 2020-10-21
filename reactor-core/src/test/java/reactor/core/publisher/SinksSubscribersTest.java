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

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.core.Disposable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

class SinksSubscribersTest {

	//arguments for the adapterRequestPatternUsesHighestServiceable test below
	private static Stream<Arguments> multicastRequestPatterns() {
		return Stream.of(
				Arguments.of(Sinks.unsafe().many().multicast().directBestEffort(), 100, 250L, 100, 3, 250L),
				Arguments.of(Sinks.unsafe().many().multicast().directAllOrNothing(), 3, 3L, 100, 100, 3L + 110L),
				Arguments.of(Sinks.unsafe().many().multicast().onBackpressureBuffer(50), 3, 50L + 3L, 100, 100, 50L + 3L + 110L),

				Arguments.of(Sinks.unsafe().many().unicast().onBackpressureBuffer(), 100, 250L, 100, 0, 250L),
				Arguments.of(Sinks.unsafe().many().unicast().onBackpressureError(), 100, 250L, 100, 0, 250L),

				Arguments.of(Sinks.unsafe().many().replay().all(), 100, 250L, 100, 100, 250L),
				Arguments.of(Sinks.unsafe().many().replay().limit(50), 53, 50L + 3L, 100, 100, 50L + 3L + 110L),
				Arguments.of(Sinks.unsafe().many().replay().latest(), 4, 1L + 3L, 100, 100, 1L + 3L + 110L)
		);
	}

	@ParameterizedTest
	@MethodSource("multicastRequestPatterns")
	void adapterRequestPatternUsesHighestServiceable(Sinks.Many<Integer> sink, int whileSlowExpectedCount, long whileSlowSourceExpectedRequest,
			int onceCaughtUpFastExpectedCount, int onceCaughtUpSlowExpectedCount, long onceCaughtUpExpectedRequest) {
		AtomicLong sourceRequested = new AtomicLong();
		boolean unicast = sink.getClass().getSimpleName().contains("Unicast");
		Flux<Integer> source = Flux.range(1, 100)
		                           .doOnRequest(sourceRequested::addAndGet)
		                           .hide();

		AssertSubscriber<Integer> downstream1 = AssertSubscriber.create(250);
		AssertSubscriber<Integer> downstream2 = AssertSubscriber.create(3);

		sink.asFlux().subscribe(downstream1);
		sink.asFlux().subscribe(downstream2);

		//TODO test parallel subscribe / requesting, implement cancel-and-recompute requestSnapshot strategy
		Disposable d = Sinks.unsafe().connectMany(source, sink);

		assertThat(downstream1.values()).as("fast count while slow").hasSize(whileSlowExpectedCount);

		if (!unicast) {
			assertThat(downstream2.values()).as("slow count while slow").containsExactly(1, 2, 3);
		}

		assertThat(sourceRequested).as("source request while slow").hasValue(whileSlowSourceExpectedRequest);

		downstream2.request(110);

		assertThat(downstream1.values()).as("fast count once caught up").hasSize(onceCaughtUpFastExpectedCount);
		assertThat(downstream2.values()).as("slow count once caught up").hasSize(onceCaughtUpSlowExpectedCount);
		downstream1.assertComplete();
		if (!unicast) {
			downstream2.assertComplete();
		}

		assertThat(sourceRequested).as("source request once caught up").hasValue(onceCaughtUpExpectedRequest);
	}

}
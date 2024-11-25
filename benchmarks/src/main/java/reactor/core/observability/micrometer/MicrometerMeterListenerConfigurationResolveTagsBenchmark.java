/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.core.instrument.Tags;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class MicrometerMeterListenerConfigurationResolveTagsBenchmark {
	@Param({"1|1", "1|2", "1|5", "1|10", "2|2", "2|5", "2|10", "5|5", "5|10", "10|10"})
	private String testCase;

	private Publisher<Void> publisher;

	@Setup(Level.Iteration)
	public void setup() {
		String[] arguments = testCase.split("\\|", -1);
		int distinctTagCount = Integer.parseInt(arguments[0]);
		int totalTagCount = Integer.parseInt(arguments[1]);

		publisher = addTags(Mono.empty(), distinctTagCount, totalTagCount);
	}

	@SuppressWarnings("unused")
	@Benchmark
	public Tags measureThroughput() {
		return MicrometerMeterListenerConfiguration.resolveTags(publisher, Tags.of("k", "v"));
	}

	private static <T> Mono<T> addTags(Mono<T> source, int distinct, int total) {
		if (total == 0) {
			return source;
		}

		return addTags(source.tag("k-" + total % distinct, "v-" + total), distinct, total - 1);
	}
}

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

package reactor.core.publisher;

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

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class TracesBenchmark {
	@Param({"0", "10", "100", "1000"})
	private int reactorLeadingLines;

	@Param({"0", "10", "100", "1000"})
	private int trailingLines;

	private String stackTrace;

	@Setup(Level.Iteration)
	public void setup() {
		stackTrace = createLargeStackTrace(reactorLeadingLines, trailingLines);
	}

	@SuppressWarnings("unused")
	@Benchmark
	public String measureThroughput() {
		return Traces.extractOperatorAssemblyInformation(stackTrace);
	}

	private static String createLargeStackTrace(int reactorLeadingLines, int trailingLines) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < reactorLeadingLines; i++) {
			sb.append("\tat reactor.core.publisher.Flux.someOperation(Flux.java:42)\n");
		}
		sb.append("\tat some.user.package.SomeUserClass.someOperation(SomeUserClass.java:1234)\n");
		for (int i = 0; i < trailingLines; i++) {
			sb.append("\tat any.package.AnyClass.anyOperation(AnyClass.java:1)\n");
		}
		return sb.toString();
	}
}

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

package reactor;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * @author Sergei Egorov
 */
@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AssemblyTraceBenchmark {

	static final String JAVA_8 = "/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home/bin/java";

	static final String JAVA_11 = "/Library/Java/JavaVirtualMachines/openjdk-11.0.1.jdk/Contents/Home/bin/java";

	@Param({"10", "40", "80"})
	int stackSize;

	@Benchmark
	@Fork(
			jvm = JAVA_8,
			jvmArgsAppend = {"-XX:-OmitStackTraceInFastThrow", "-Dreactor.trace.operatorStacktrace=true"}
	)
	public void withTracingOnJDK8(Blackhole bh) {
		stack(stackSize, bh);
	}

	@Benchmark
	@Fork(
			jvm = JAVA_11,
			jvmArgsAppend = {"-XX:-OmitStackTraceInFastThrow", "-Dreactor.trace.operatorStacktrace=true"}
	)
	public void withTracingOnJDK11(Blackhole bh) {
		stack(stackSize, bh);
	}

	@Benchmark
	@Fork(jvmArgsAppend = "-Dreactor.trace.operatorStacktrace=false")
	public void withoutTracing(Blackhole bh) {
		stack(stackSize, bh);
	}

	private void stack(int i, Blackhole bh) {
		if (i == 0) {
			Object result = Flux.just(1)
			                    .map(__ -> {
				                    throw new IllegalStateException();
			                    })
			                    .share()
			                    .filter(d -> true)
			                    .doOnNext(d -> {
			                    })
			                    .map(d -> d)
			                    .materialize()
			                    .blockLast();

			bh.consume(result);
		} else {
			stack(i - 1, bh);
		}
	}
}

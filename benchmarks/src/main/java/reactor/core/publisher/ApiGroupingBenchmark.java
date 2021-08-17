/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author Simon Baslé
 */
@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ApiGroupingBenchmark {

	@Benchmark
	public void transformative_baseline(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.buffer(4)
			.buffer(2)
		);
	}

	@Benchmark
	public void transformative_instancePerCall(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.buffers_v1()
			.fixedSize(4)
			.buffers_v1()
			.fixedSize(2)
		);
	}

	@Benchmark
	public void transformative_builder(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.buffers_v2()
			.fixedSize(4)
			.fixedSize(2)
			.generateFlux()
		);
	}

	@Benchmark
	public void transformative_builderConsumer(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.buffers_v3(configure -> configure.fixedSize(4).fixedSize(2))
		);
	}

	@Benchmark
	public void identity_baseline(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.doOnComplete(() -> bh.consume(1))
			.doOnNext(bh::consume)
		);
	}

	@Benchmark
	public void identity_instancePerCall(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.sideEffects_v1()
			.doOnComplete(() -> bh.consume(1))
			.sideEffects_v1()
			.doOnNext(bh::consume)
		);
	}

	@Benchmark
	public void identity_builder(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.sideEffects_v2()
			.doOnComplete(() -> bh.consume(1))
			.doOnNext(bh::consume)
			.endSideEffects()
		);
	}

	@Benchmark
	public void identity_builderConsumer(Blackhole bh) {
		bh.consume(Flux.range(1, 10)
			.sideEffects_v3(configure -> configure.doOnComplete(() -> bh.consume(1)).doOnNext(bh::consume))
		);
	}
}
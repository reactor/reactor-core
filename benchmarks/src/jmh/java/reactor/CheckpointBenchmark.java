/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import org.openjdk.jmh.annotations.Warmup;
import reactor.core.publisher.Flux;

/**
 * @author Simon Baslé
 */
public class CheckpointBenchmark {

	Flux<String> findAllUserByName(Flux<String> source) {
		return source.map(s -> { throw new IllegalStateException("boom"); })
		             .map(s -> s + "-user");
	}

	@Benchmark()
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, time = 1)
	@Measurement(iterations = 5, time = 1)
	@Fork(1)
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void withFullCheckpoint() {
		this.findAllUserByName(Flux.just("pedro", "simon", "stephane"))
		    .transform(f -> f.filter(s -> s.startsWith("s")))
		    .transform(f -> f.elapsed())
		    .checkpoint("checkpoint description", true)
		    .subscribe(System.out::println, t -> {
		    });
	}

	@Benchmark
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, time = 1)
	@Measurement(iterations = 5, time = 1)
	@Fork(1)
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void withLightCheckpoint() {
		this.findAllUserByName(Flux.just("pedro", "simon", "stephane"))
		    .transform(f -> f.filter(s -> s.startsWith("s")))
		    .transform(f -> f.elapsed())
		    .checkpoint("light checkpoint identifier")
		    .subscribe(System.out::println, t -> {
		    });
	}
}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import reactor.core.publisher.Flux;
import reactor.guide.FakeRepository;
import reactor.guide.FakeUtils1;
import reactor.guide.FakeUtils2;

/**
 * @author Simon Baslé
 */
public class CheckpointBenchmark {

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void withFullCheckpoint() {
		FakeRepository.findAllUserByName(Flux.just("pedro", "simon", "stephane"))
		              .transform(FakeUtils1.applyFilters)
		              .transform(FakeUtils2.enrichUser)
		              .checkpoint("checkpoint description", true)
		              .subscribe(System.out::println,
		                      t -> {}
                      );
	}

	@Benchmark
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void withLightCheckpoint() {
		FakeRepository.findAllUserByName(Flux.just("pedro", "simon", "stephane"))
		              .transform(FakeUtils1.applyFilters)
		              .transform(FakeUtils2.enrichUser)
		              .checkpoint("light checkpoint identifier")
		              .subscribe(System.out::println,
				              t -> {}
		              );
	}
}

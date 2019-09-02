/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 10, time = 5)
@Measurement(iterations = 10, time = 5)
@Fork(value = 2)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RateLimiterBenchmark {

    Flux<Integer> source;

    @Setup(Level.Trial)
    public void setup() {
        source = Flux.range(0, 1000000).hide();
    }

    @Benchmark
    public void publishOnRateLimiting() {
        source
            .limitRate(256, 256, false)
            .blockLast();

    }

    @Benchmark
    public void nativeRateLimiting() {
        source
            .limitRate(256, 256, true)
            .blockLast();
    }

    public static void main(String[] args) throws IOException, RunnerException {
        Main.main(args);
    }
}

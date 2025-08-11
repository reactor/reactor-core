/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author Simon Baslé
 */
@State(Scope.Benchmark)
public class BoundedElasticBusyStructureBenchmark {

	@SuppressWarnings("NullAway.Init")
	OldBoundedElasticScheduler oldBoundedElasticScheduler;

	@SuppressWarnings("NullAway.Init")
	Scheduler                  newBoundedElasticScheduler;

	@Setup(Level.Trial)
	public void setup() {
		final AtomicInteger oldCounter = new AtomicInteger();
		ThreadFactory oldFactory = r -> new Thread(r, "oldBounded-" + oldCounter.incrementAndGet());
		oldBoundedElasticScheduler = new OldBoundedElasticScheduler(100, 10_000, oldFactory, 60 * 1000);
		oldBoundedElasticScheduler.start();

		newBoundedElasticScheduler = Schedulers.newBoundedElastic(100, 10_000, "newBounded", 60, false);
	}

	@Benchmark
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, time = 1)
	@Measurement(iterations = 10, time = 1)
	@Threads(10)
	@BenchmarkMode({Mode.AverageTime})
	public void withArray(Blackhole bh) throws InterruptedException {
		int runs = 1000;
		CountDownLatch latch = new CountDownLatch(runs);
		for (int i = 0; i < runs; i++) {
			newBoundedElasticScheduler.schedule(latch::countDown);
		}
		bh.consume(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@Benchmark()
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, time = 1)
	@Measurement(iterations = 10, time = 1)
	@Threads(10)
	@BenchmarkMode({Mode.AverageTime})
	public void withPriorityQueue(Blackhole bh) throws InterruptedException {
		int runs = 1000;
		CountDownLatch latch = new CountDownLatch(runs);
		for (int i = 0; i < runs; i++) {
			oldBoundedElasticScheduler.schedule(latch::countDown);
		}
		bh.consume(latch.await(100, TimeUnit.MILLISECONDS));
	}
}

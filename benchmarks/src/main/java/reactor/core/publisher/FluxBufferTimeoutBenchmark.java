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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FluxBufferTimeoutBenchmark {

	private static final int TOTAL_VALUES = 100;

	@Param({"1", "10", "100"})
	int bufferSize;

	@Benchmark
	public Object unlimited(Blackhole blackhole) throws InterruptedException {
		JmhSubscriber subscriber = new JmhSubscriber(blackhole, false);
		Flux.range(0, TOTAL_VALUES)
				.bufferTimeout(bufferSize, Duration.ofDays(100), true)
				.subscribe(subscriber);
		subscriber.await();
		return subscriber;
	}

	@Benchmark
	public Object oneByOne(Blackhole blackhole) throws InterruptedException {
		JmhSubscriber subscriber = new JmhSubscriber(blackhole, true);
		Flux.range(0, TOTAL_VALUES)
				.bufferTimeout(bufferSize, Duration.ofDays(100), true)
				.subscribe(subscriber);
		subscriber.await();
		return subscriber;
	}


	public static class JmhSubscriber extends CountDownLatch
			implements CoreSubscriber<List<Integer>> {

		private final Blackhole    blackhole;
		private final boolean      oneByOneRequest;
		// Initialized in onSubscribe(). Usage happens post-initialization, e.g. `this`
		// is only presented to downstream as a result of onSubscibe().
		@SuppressWarnings("NullAway.Init")
		private       Subscription s;

		public JmhSubscriber(Blackhole blackhole, boolean oneByOneRequest) {
			super(1);
			this.blackhole = blackhole;
			this.oneByOneRequest = oneByOneRequest;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (oneByOneRequest) {
					s.request(1);
				} else {
					s.request(Long.MAX_VALUE);
				}
			}
		}

		@Override
		public void onNext(List<Integer> t) {
			blackhole.consume(t);
			if (oneByOneRequest) {
				s.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			blackhole.consume(t);
			countDown();
		}

		@Override
		public void onComplete() {
			countDown();
		}
	}
}

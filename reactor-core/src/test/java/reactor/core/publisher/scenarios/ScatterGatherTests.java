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

package reactor.core.publisher.scenarios;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ScatterGatherTests {

	@Test
	public void test() throws Exception {

		Flux.just("red", "white", "blue")
		    .log("source")
		    .flatMap(value -> Mono.fromCallable(() -> {
								    Thread.sleep(1000);
								    return value;
							    }).subscribeOn(Schedulers.boundedElastic()))
		    .log("merged")
		    .collect(Result::new, Result::add)
		    .doOnNext(Result::stop)
		    .log("accumulated")
		    .toFuture()
			.get();
	}

	final class Result {

		private ConcurrentMap<String, AtomicLong> counts = new ConcurrentHashMap<>();

		private long timestamp = System.currentTimeMillis();

		private long duration;

		public long add(String colour) {
			AtomicLong value = counts.getOrDefault(colour, new AtomicLong());
			counts.putIfAbsent(colour, value);
			return value.incrementAndGet();
		}

		public void stop() {
			this.duration = System.currentTimeMillis() - timestamp;
		}

		public long getDuration() {
			return duration;
		}

		public Map<String, AtomicLong> getCounts() {
			return counts;
		}

		@Override
		public String toString() {
			return "Result [duration=" + duration + ", counts=" + counts + "]";
		}

	}

}

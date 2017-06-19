/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;

import org.junit.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class ParallelMergeReduceTest {

	@Test
	public void reduceFull() {
		for (int i = 1;
		     i <= Runtime.getRuntime()
		                 .availableProcessors() * 2;
		     i++) {
			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			Flux.range(1, 10)
			    .parallel(i)
			    .reduce((a, b) -> a + b)
			    .subscribe(ts);

			ts.assertValues(55);
		}
	}

	@Test
	public void parallelReduceFull() {
		int m = 100_000;
		for (int n = 1; n <= m; n *= 10) {
//            System.out.println(n);
			for (int i = 1;
			     i <= Runtime.getRuntime()
			                 .availableProcessors();
			     i++) {
//                System.out.println("  " + i);

				Scheduler scheduler = Schedulers.newParallel("test", i);

				try {
					AssertSubscriber<Long> ts = AssertSubscriber.create();

					Flux.range(1, n)
					    .map(v -> (long) v)
					    .parallel(i)
					    .runOn(scheduler)
					    .reduce((a, b) -> a + b)
					    .subscribe(ts);

					ts.await(Duration.ofSeconds(500));

					long e = ((long) n) * (1 + n) / 2;

					ts.assertValues(e);
				}
				finally {
					scheduler.dispose();
				}
			}
		}
	}
}

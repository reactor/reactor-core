/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxIntervalTest {

	TimedScheduler exec;

	@Before
	public void before() {
		exec = Schedulers.newTimer("interval-test");
	}

	@After
	public void after() {
		exec.dispose();
	}

	@Test
	public void normal() {
		try {
			AssertSubscriber<Long> ts = AssertSubscriber.create();

			ts.values()
			  .add(System.currentTimeMillis());

			Flux.intervalMillis(100, 100, exec)
			    .take(5)
			    .map(v -> System.currentTimeMillis())
			    .subscribe(ts);

			ts.await(Duration.ofSeconds(5));

			ts.assertValueCount(5)
			  .assertNoError()
			  .assertComplete();

			List<Long> list = ts.values();
			for (int i = 0; i < list.size() - 1; i++) {
				long diff = list.get(i + 1) - list.get(i);

				if (diff < 50 || diff > 150) {
					Assert.fail("Period failure: " + diff);
				}
			}

		}
		finally {
			exec.dispose();
		}
	}

	Flux<Integer> flatMapScenario() {
		return Flux.interval(Duration.ofSeconds(3))
		    .flatMap(v -> Flux.fromIterable(Arrays.asList("A"))
		                      .flatMap(w -> Mono.fromCallable(() -> Arrays.asList(1, 2))
		                                        .subscribeOn(Schedulers.timer())
		                                        .flatMap(Flux::fromIterable))).log();
	}

	@Test
	public void flatMap() throws Exception {
		StepVerifier.withVirtualTime(this::flatMapScenario)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext(1)
		            .expectNext(2)
		            .thenCancel()
		            .verify();
	}
}

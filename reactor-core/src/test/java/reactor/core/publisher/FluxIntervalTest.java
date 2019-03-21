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

package reactor.core.publisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxIntervalTest {

	Scheduler exec;

	@Before
	public void before() {
		exec = Schedulers.newSingle("interval-test");
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

			Flux.interval(Duration.ofMillis(100), Duration.ofMillis(100), exec)
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
		                                        .subscribeOn(Schedulers.parallel())
		                                        .flatMapMany(Flux::fromIterable))).log();
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

	Flux<Long> scenario2(){
		return Flux.interval(Duration.ofMillis(500));
	}

	@Test
	public void normal2() {
		StepVerifier.withVirtualTime(this::scenario2)
		            .thenAwait(Duration.ofMillis(5_000))
		            .expectNextCount(10)
		            .thenCancel()
		            .verify();
	}

	Flux<Long> scenario3(){
		return Flux.interval(Duration.ofMillis(500), Duration.ofMillis(1000));
	}

	@Test
	public void normal3() {
		StepVerifier.withVirtualTime(this::scenario3)
		            .thenAwait(Duration.ofMillis(1500))
		            .expectNext(0L)
		            .thenAwait(Duration.ofSeconds(4))
		            .expectNextCount(4)
		            .thenCancel()
		            .verify();
	}

	Flux<Long> scenario4(){
		return Flux.interval(Duration.ofMillis(500), Duration.ofMillis(1000));
	}

	@Test
	public void normal4() {
		StepVerifier.withVirtualTime(this::scenario4)
		            .thenAwait(Duration.ofMillis(1500))
		            .expectNext(0L)
		            .thenAwait(Duration.ofSeconds(4))
		            .expectNextCount(4)
		            .thenCancel()
		            .verify();
	}

	@Test
    public void scanIntervalRunnable() {
		Scheduler.Worker worker = Schedulers.single().createWorker();

		try {
        CoreSubscriber<Long> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxInterval.IntervalRunnable test = new FluxInterval.IntervalRunnable(actual, worker);

        assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(worker);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		}
		finally {
			worker.dispose();
		}
    }

    @Test
    public void scanOperator() {
	    final Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));

	    assertThat(interval).isInstanceOf(Scannable.class);
	    assertThat(((Scannable) interval).scan(Scannable.Attr.RUN_ON))
			    .isSameAs(Schedulers.parallel());
    }

    @Test
	public void tickOverflow() {
		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofMillis(50))
				    .delayUntil(i -> Mono.delay(Duration.ofMillis(250))))
		            .thenAwait(Duration.ofMinutes(1))
		            .expectNextCount(6)
		            .verifyErrorMessage("Could not emit tick 32 due to lack of requests (interval doesn't support small downstream requests that replenish slower than the ticks)");
    }

    @Test
	public void shouldBeAbleToScheduleIntervalsWithLowGranularity() {
		StepVerifier.create(Flux.interval(Duration.ofNanos(1)))
		            .expectSubscription()
		            .expectNext(0L)
		            .expectNext(1L)
		            .expectNext(2L)
		            .thenCancel()
		            .verify();
    }
}

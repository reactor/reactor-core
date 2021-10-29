/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FluxGroupByTest extends
                             FluxOperatorTest<String, GroupedFlux<Integer, String>> {

	@Test
	@Tag("slow")
	//see https://github.com/reactor/reactor-core/issues/2730
	void performanceOfContinuouslyCancellingGroups() throws Exception {
		AtomicLong upstream = new AtomicLong(0L);
		AtomicLong downstream = new AtomicLong(0L);
		CountDownLatch latch = new CountDownLatch(1);

		Scheduler scheduler = Schedulers.single(Schedulers.boundedElastic());

		Flux.fromStream(Stream.iterate(0L, (last) -> (long) (Math.random() * 4096)))
				.flatMap(number -> Flux.concat(
						Mono.just(number),
						Mono.just(number).delayElement(Duration.ofMillis((int) (Math.random() * 2000)))),
						4096)
				.take(Duration.ofSeconds(10L))
				.doOnNext(next -> upstream.incrementAndGet())
				.publishOn(scheduler)
				.groupBy(Function.identity())
				.flatMap(groupFlux -> groupFlux.take(Duration.ofSeconds(1L), scheduler)
						.take(2, false)
						.collectList(), 16384)
				.map(Collection::size)
				.subscribe(downstream::addAndGet, System.err::println, latch::countDown);

		latch.await();
		assertThat(upstream).as("upstream and downstream consistent").hasValue(downstream.get());
		assertThat(downstream).as("order of magnitude").hasValueGreaterThan(30_000);
	}

	@Override
	protected Scenario<String, GroupedFlux<Integer, String>> defaultScenarioOptions(Scenario<String, GroupedFlux<Integer, String>> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ASYNC)
		                     .fusionModeThreadBarrier(Fuseable.ANY)
		                     .prefetch(Queues.SMALL_BUFFER_SIZE)
		                     .shouldAssertPostTerminateState(false);
	}

	@Override
	protected List<Scenario<String, GroupedFlux<Integer, String>>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.groupBy(String::hashCode))
		);
	}

	@Override
	protected List<Scenario<String, GroupedFlux<Integer, String>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.groupBy(String::hashCode))
					.receive(g -> assertThat(g.key()).isEqualTo(g.blockFirst().hashCode()),
							    g -> assertThat(g.key()).isEqualTo(g.blockFirst().hashCode()),
							    g -> assertThat(g.key()).isEqualTo(g.blockFirst().hashCode()))
		);
	}

	@Override
	protected List<Scenario<String, GroupedFlux<Integer, String>>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.groupBy(String::hashCode, s -> {
					throw exception();
				})),

				scenario(f -> f.groupBy(s -> {
					throw exception();
				})),

				scenario(f -> f.groupBy(String::hashCode, s -> null))
						,

				scenario(f -> f.groupBy(k -> null))

		);
	}

	@Test
	void gh2675() {
		StepVerifier.create(
				Flux.just(0, 1, 2, 3)
				    .log()
				    .groupBy(f -> f / 2, 1)
				    .flatMap(a -> a.log("group " + a.key()).takeUntil(i -> i % 2 == 1)))
		            .expectNext(0, 1, 2, 3)
		            .expectComplete()
		            .verify(Duration.ofMillis(5000));
	}

	@Test
	public void normal() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(1, 3, 5, 7, 9);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(2, 4, 6, 8, 10);
	}

	@Test
	public void normalValueSelector() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2, v -> -v)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(-1, -3, -5, -7, -9);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(-2, -4, -6, -8, -10);
	}

	@Test
	public void takeTwoGroupsOnly() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 3)
		    .take(2, false)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(1, 4, 7, 10);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(2, 5, 8);
	}

	@Test
	public void keySelectorNull() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> (Integer) null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class);
	}

	@Test
	public void valueSelectorNull() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> 1, v -> (Integer) null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class);
	}

	@Test
	public void error() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).groupBy(k -> k)
		                                                           .subscribe(ts);

		ts.assertErrorMessage("forced failure");
	}

	@Test
	public void backpressure() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create(0L);

		Flux.range(1, 10)
		    .groupBy(k -> 1)
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0L);

		ts.values()
		  .get(0)
		  .subscribe(ts1);

		ts1.assertNoEvents();

		ts1.request(10);

		ts1.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void flatMapBack() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .flatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void flatMapBackHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .flatMap(g -> g.hide())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void concatMapBack() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .concatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
	}

	@Test
	public void concatMapBackHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .hide()
		    .concatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
	}

	@Test
	public void empty() {
		AssertSubscriber<GroupedFlux<Integer, Integer>> ts = AssertSubscriber.create(0L);

		Flux.<Integer>empty().groupBy(v -> v)
		                     .subscribe(ts);

		ts.assertValues();
	}

	@Test
	public void oneGroupLongMerge() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> 1)
		    .flatMap(g -> g)
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void oneGroupLongMergeHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> 1)
		    .flatMap(g -> g.hide())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongMerge() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g)
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongMergeHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g.hide())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongAsyncMerge() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g)
		    .publishOn(Schedulers.fromExecutorService(forkJoinPool))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongAsyncMergeHidden() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g.hide())
		    .publishOn(Schedulers.fromExecutorService(forkJoinPool))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	@Tag("slow")
	public void twoGroupsLongAsyncMergeHidden2() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();

		for (int j = 0; j < 100; j++) {
			AssertSubscriber<Long> ts = AssertSubscriber.create();
			AtomicLong dropped = new AtomicLong();

			Hooks.onNextDropped(__ -> dropped.incrementAndGet());
			try {
				final int total = 100_000;
				Flux.range(0, total)
				    .groupBy(i -> (i / 2d) * 2d, 42)
				    .flatMap(it -> it.take(1, false)
				                     .hide(), 2)
				    .publishOn(Schedulers.fromExecutorService(forkJoinPool), 2)
				    .count()
				    .subscribe(ts);

				ts.await(Duration.ofSeconds(50));

				ts.assertValues(total - dropped.get())
				  .assertNoError()
				  .assertComplete();

			} finally {
				Hooks.resetOnNextDropped();
			}
		}
	}

	@Test
	public void twoGroupsConsumeWithSubscribe() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		ts1.await(Duration.ofSeconds(5));
		ts2.await(Duration.ofSeconds(5));
		ts3.await(Duration.ofSeconds(5));

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	public void twoGroupsConsumeWithSubscribePrefetchSmaller() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool),
							    32)
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool),
							    32)
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		if (!ts1.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("main subscriber timed out");
		}
		if (!ts2.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("group 0 subscriber timed out");
		}
		if (!ts3.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("group 1 subscriber timed out");
		}

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	public void twoGroupsConsumeWithSubscribePrefetchBigger() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool),
							    1024)
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool),
							    1024)
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		if (!ts1.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("main subscriber timed out");
		}
		if (!ts2.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("group 0 subscriber timed out");
		}
		if (!ts3.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			fail("group 1 subscriber timed out");
		}

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	public void twoGroupsConsumeWithSubscribeHide() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts1);
				    }
				    else {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		ts1.await(Duration.ofSeconds(5));
		ts2.await(Duration.ofSeconds(5));
		ts3.await(Duration.ofSeconds(5));

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	@Disabled("temporarily disabled, see gh-1028")
	public void twoGroupsFullAsyncFullHide() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(forkJoinPool))
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts1);
				    }
				    else {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		ts1.await(Duration.ofSeconds(5));
		ts2.await(Duration.ofSeconds(5));
		ts3.await(Duration.ofSeconds(5));

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	@Disabled("temporarily disabled, see gh-1028")
	public void twoGroupsFullAsync() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts3 = AssertSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .publishOn(Schedulers.fromExecutorService(forkJoinPool), 512)
		    .groupBy(v -> v & 1)
		    .subscribe(new CoreSubscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(forkJoinPool))
					     .subscribe(ts2);
				    }
			    }

			    @Override
			    public void onError(Throwable t) {
				    ts3.onError(t);
			    }

			    @Override
			    public void onComplete() {
				    ts3.onComplete();
			    }
		    });

		ts1.await(Duration.ofSeconds(5));
		ts2.await(Duration.ofSeconds(5));
		ts3.await(Duration.ofSeconds(5));

		ts1.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts2.assertValueCount(500_000)
		   .assertNoError()
		   .assertComplete();

		ts3.assertNoValues()
		   .assertNoError()
		   .assertComplete();

	}

	@Test
	public void groupsCompleteAsSoonAsMainCompletes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(0, 20)
		    .groupBy(i -> i % 5)
		    .concatMap(v -> v, 2)
		    .subscribe(ts);

		ts.assertValues(0,
				5,
				10,
				15,
				1,
				6,
				11,
				16,
				2,
				7,
				12,
				17,
				3,
				8,
				13,
				18,
				4,
				9,
				14,
				19)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void groupsCompleteAsSoonAsMainCompletesNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(0, 20)
		    .groupBy(i -> i % 5)
		    .hide()
		    .concatMap(v -> v, 2)
		    .subscribe(ts);

		ts.assertValues(0,
				5,
				10,
				15,
				1,
				6,
				11,
				16,
				2,
				7,
				12,
				17,
				3,
				8,
				13,
				18,
				4,
				9,
				14,
				19)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void prefetchIsUsed() {
		AtomicLong initialRequest = new AtomicLong();

		StepVerifier.create(Flux.range(1, 10)
		                        .doOnRequest(r -> initialRequest.compareAndSet(0L, r))
		                        .groupBy(i -> i % 5, 11)
		                        .concatMap(v -> v))
		            .expectNextCount(10)
		            .verifyComplete();

		assertThat(initialRequest).hasValue(11);
	}

	@Test
	public void prefetchMaxRequestsUnbounded() {
		AtomicLong initialRequest = new AtomicLong();

		StepVerifier.create(Flux.range(1, 10)
		                        .doOnRequest(r -> initialRequest.compareAndSet(0L, r))
		                        .groupBy(i -> i % 5, Integer.MAX_VALUE)
		                        .concatMap(v -> v))
		            .expectNextCount(10)
		            .verifyComplete();

		assertThat(initialRequest).hasValue(Long.MAX_VALUE);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxGroupBy<Integer, Integer, Integer> test
				= new FluxGroupBy<>(parent, k -> k % 2, v -> -v, Queues.unbounded(3), Queues.unbounded(3), 3);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMain() {
		CoreSubscriber<GroupedFlux<Integer, String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxGroupBy.GroupByMain<Integer, Integer, String> test = new FluxGroupBy.GroupByMain<>(actual,
				Queues.<GroupedFlux<Integer, String>>one().get(), Queues.one(), 123, i -> i % 5, i -> String.valueOf(i));
		Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isSameAs(123);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isSameAs(0);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
	}

	@Test
	public void scanUnicastGroupedFlux() {
		CoreSubscriber<GroupedFlux<Integer, String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxGroupBy.GroupByMain<Integer, Integer, String> main = new FluxGroupBy.GroupByMain<>(actual,
				Queues.<GroupedFlux<Integer, String>>one().get(), Queues.one(), 123, i -> i % 5, i -> String.valueOf(i));
		FluxGroupBy.UnicastGroupedFlux<Integer, String> test = new FluxGroupBy.UnicastGroupedFlux<Integer, String>(1,
				Queues.<String>one().get(), main, 123);
		CoreSubscriber<String> sub = new LambdaSubscriber<>(null, e -> {}, null, null);
        test.subscribe(sub);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isSameAs(0);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
	}

	@Test
	@Timeout(10)
	public void fusedGroupByParallel() {
		int parallelism = 2;
		Scheduler process = Schedulers.newParallel("process", parallelism, true);

		final long start = System.nanoTime();

		Flux.range(0, 500_000)
				.subscribeOn(Schedulers.newSingle("range", true))
				.groupBy(i -> i % 2)
				.flatMap(g ->
						g.key() == 0
								? g //.hide()  /* adding hide here fixes the hang issue */
								.parallel(parallelism)
								.runOn(process)
								.map(i -> i)
								.sequential()
								: g.map(i -> i) // no need to use hide
				)
				.then()
				.block();
	}
}

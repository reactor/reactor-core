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
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Schedulers;
import reactor.test.TestSubscriber;
import reactor.util.concurrent.QueueSupplier;

public class FluxGroupByTest {

	@Test
	public void normal() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(1, 3, 5, 7, 9);

		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(2, 4, 6, 8, 10);
	}

	@Test
	public void normalValueSelector() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2, v -> -v)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(-1, -3, -5, -7, -9);

		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(-2, -4, -6, -8, -10);
	}

	@Test
	public void takeTwoGroupsOnly() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 3)
		    .take(2)
		    .subscribe(ts);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		ts.values()
		  .get(0)
		  .subscribe(ts1);
		ts1.assertValues(1, 4, 7, 10);

		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		ts.values()
		  .get(1)
		  .subscribe(ts2);
		ts2.assertValues(2, 5, 8);
	}

	@Test
	public void keySelectorNull() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> (Integer) null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class);
	}

	@Test
	public void valueSelectorNull() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> 1, v -> (Integer) null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class);
	}

	@Test
	public void error() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).groupBy(k -> k)
		                                                           .subscribe(ts);

		ts.assertErrorMessage("forced failure");
	}

	@Test
	public void backpressure() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create(0L);

		Flux.range(1, 10)
		    .groupBy(k -> 1)
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		TestSubscriber<Integer> ts1 = TestSubscriber.create(0L);

		ts.values()
		  .get(0)
		  .subscribe(ts1);

		ts1.assertNoEvents();

		ts1.request(10);

		ts1.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void flatMapBack() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .flatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void flatMapBackHidden() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .flatMap(g -> g.hide())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void concatMapBack() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .concatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
	}

	@Test
	public void concatMapBackHidden() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .groupBy(k -> k % 2)
		    .hide()
		    .concatMap(g -> g)
		    .subscribe(ts);

		ts.assertValues(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
	}

	@Test
	public void empty() {
		TestSubscriber<GroupedFlux<Integer, Integer>> ts = TestSubscriber.create(0L);

		Flux.<Integer>empty().groupBy(v -> v)
		                     .subscribe(ts);

		ts.assertValues();
	}

	@Test
	public void oneGroupLongMerge() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g.hide())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongAsyncMergeLoop() {
		for (int i = 0; i < 100; i++) {
			twoGroupsLongAsyncMerge();
		}
	}

	@Test
	public void twoGroupsLongAsyncMerge() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g)
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsLongAsyncMergeHiddenLoop() {
		for (int i = 0; i < 100; i++) {
			twoGroupsLongAsyncMergeHidden();
		}
	}

	@Test
	public void twoGroupsLongAsyncMergeHidden() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 1_000_000)
		    .groupBy(v -> (v & 1))
		    .flatMap(g -> g.hide())
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoGroupsConsumeWithSubscribe() {

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
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

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()),
							    32)
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()),
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
			Assert.fail("main subscriber timed out");
		}
		if (!ts2.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			Assert.fail("group 0 subscriber timed out");
		}
		if (!ts3.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			Assert.fail("group 1 subscriber timed out");
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

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()),
							    1024)
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()),
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
			Assert.fail("main subscriber timed out");
		}
		if (!ts2.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			Assert.fail("group 0 subscriber timed out");
		}
		if (!ts3.await(Duration.ofSeconds(5))
		        .isTerminated()) {
			Assert.fail("group 1 subscriber timed out");
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

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
					     .subscribe(ts1);
				    }
				    else {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
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
	public void twoGroupsFullAsyncFullHide() {

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
					     .subscribe(ts1);
				    }
				    else {
					    t.hide()
					     .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
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
	public void twoGroupsFullAsync() {

		TestSubscriber<Integer> ts1 = TestSubscriber.create();
		TestSubscriber<Integer> ts2 = TestSubscriber.create();
		TestSubscriber<Integer> ts3 = TestSubscriber.create();
		ts3.onSubscribe(Operators.emptySubscription());

		Flux.range(0, 1_000_000)
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()), 512)
		    .groupBy(v -> v & 1)
		    .subscribe(new Subscriber<GroupedFlux<Integer, Integer>>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(Long.MAX_VALUE);
			    }

			    @Override
			    public void onNext(GroupedFlux<Integer, Integer> t) {
				    if (t.key() == 0) {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
					     .subscribe(ts1);
				    }
				    else {
					    t.publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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

}

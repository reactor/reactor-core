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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscribeOnTest {
	
	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxPublishOn.class);
		
		ctb.addRef("source", Flux.never());
		ctb.addRef("executor", Schedulers.single());
		ctb.addRef("schedulerFactory", (Callable<? extends Consumer<Runnable>>)() -> r -> { });
		
		ctb.test();
	}*/

	@Test
	public void classic() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.fromSupplier(() -> 1)
		    .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void classicBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.fromCallable(() -> 1)
		    .log()
		    .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		Thread.sleep(100);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		Thread.sleep(1000);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		ts.request(500);
	}

	@Test
	public void classicJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void classicJustBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		Thread.sleep(100);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void classicEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>empty().subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		                     .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void classicEmptyBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>empty().subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		                     .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void callableEvaluatedTheRightTime() {

		AtomicInteger count = new AtomicInteger();

		Mono<Integer> p = Mono.fromCallable(count::incrementAndGet)
		                      .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()));

		Assert.assertEquals(0, count.get());

		p.subscribeWith(AssertSubscriber.create())
		 .await();

		Assert.assertEquals(1, count.get());
	}

	@Test
	public void scanSubscribeOnSubscriber() {
		final Flux<String> source = Flux.just("foo");
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoSubscribeOn.SubscribeOnSubscriber<String> test = new MonoSubscribeOn.SubscribeOnSubscriber<>(
				source, actual, Schedulers.single().createWorker());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.requested = 3L;
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}

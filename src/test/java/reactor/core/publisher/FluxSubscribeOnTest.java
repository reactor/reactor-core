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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

public class FluxSubscribeOnTest {

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

		Flux.range(1, 1000).subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void classicBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1000).log().subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

		Thread.sleep(100);

		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts.request(500);

		Thread.sleep(1000);

		ts.assertValueCount(500)
		.assertNoError()
		.assertNotComplete();

		ts.request(500);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void classicJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1).subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void classicJustBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1).subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

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

		Flux.<Integer>empty().subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void classicEmptyBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool())).subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}


	@Test
	public void callableEvaluatedTheRightTime() {

		AtomicInteger count = new AtomicInteger();

		Mono<Integer> p = Mono.fromCallable(count::incrementAndGet).subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()));

		Assert.assertEquals(0, count.get());

		p.subscribeWith(AssertSubscriber.create()).await();

		Assert.assertEquals(1, count.get());
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSubscribeOn.SubscribeOnSubscriber<Integer> test =
        		new FluxSubscribeOn.SubscribeOnSubscriber<>(Flux.just(1), actual, Schedulers.single().createWorker());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}

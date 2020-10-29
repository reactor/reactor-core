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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscribeOnTest {

	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

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

		Thread.sleep(2000);

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
	public void classicWithTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		Mono.fromCallable(() -> {
			try {
				TimeUnit.SECONDS.sleep(2L);
			}
			catch (InterruptedException ignore) {
			}
			return 0;
		})
		    .timeout(Duration.ofMillis(100L))
		    .onErrorResume(t -> Mono.fromCallable(() -> 1))
		    .subscribeOn(afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "timeout")))
		    .subscribe(ts);

		ts.request(1);

		ts.await(Duration.ofMillis(400))
		  .assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void callableEvaluatedTheRightTime() {

		AtomicInteger count = new AtomicInteger();

		Mono<Integer> p = Mono.fromCallable(count::incrementAndGet)
		                      .subscribeOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()));

		assertThat(count).hasValue(0);

		p.subscribeWith(AssertSubscriber.create())
		 .await();

		assertThat(count).hasValue(1);
	}

	@Test
	public void scanOperator() {
		MonoSubscribeOn<String> test = new MonoSubscribeOn<>(Mono.empty(), Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscribeOnSubscriber() {
		Scheduler.Worker worker = Schedulers.single().createWorker();

		try {
			final Flux<String> source = Flux.just("foo");
			CoreSubscriber<String>
					actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
			MonoSubscribeOn.SubscribeOnSubscriber<String> test = new MonoSubscribeOn.SubscribeOnSubscriber<>(
					source, actual, worker);
			Subscription parent = Operators.emptySubscription();
			test.onSubscribe(parent);

			test.requested = 3L;
			assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);

			assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(worker);
			assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
			assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
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
	public void error() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .subscribeOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .subscribeOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void disposeWorkerIfCancelledBeforeOnSubscribe() {
		AtomicInteger disposeCount = new AtomicInteger();
		Scheduler.Worker countingWorker = new Scheduler.Worker() {
			@Override
			public Disposable schedule(Runnable task) {
				return Disposables.disposed();
			}

			@Override
			public void dispose() {
				disposeCount.incrementAndGet();
			}
		};
		MonoSubscribeOn.SubscribeOnSubscriber<Integer> sosub =
				new MonoSubscribeOn.SubscribeOnSubscriber<>(ignoredSubscribe -> {}, null, countingWorker);
		for (int i = 1; i <= 10_000; i++) {
			RaceTestUtils.race(sosub::cancel, () -> sosub.onSubscribe(Operators.emptySubscription()));
			assertThat(disposeCount).as("idle/disposed in round %d", i).hasValue(i);

			//reset
			sosub.s = null;
		}
	}
}

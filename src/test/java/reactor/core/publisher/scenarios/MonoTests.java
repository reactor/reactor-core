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
package reactor.core.publisher.scenarios;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 */
public class MonoTests {

	@Test
	public void testDoOnEachSignal() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		Mono<Integer> mono = Mono.just(1)
		                         .doOnEach(signals::add);
		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertThat(signals.size(), is(2));
		assertThat("onNext", signals.get(0).get(), is(1));
		assertTrue("onComplete expected", signals.get(1).isOnComplete());
	}

	@Test
	public void testDoOnEachEmpty() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		Mono<Integer> mono = Mono.<Integer>empty()
		                         .doOnEach(signals::add);
		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectComplete()
		            .verify();

		assertThat(signals.size(), is(1));
		assertTrue("onComplete expected", signals.get(0).isOnComplete());

	}

	@Test
	public void testDoOnEachSignalWithError() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		Mono<Integer> mono = Mono.<Integer>error(new IllegalArgumentException("foo"))
				.doOnEach(signals::add);
		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectErrorMessage("foo")
		            .verify();

		assertThat(signals.size(), is(1));
		assertTrue("onError expected", signals.get(0).isOnError());
		assertThat("plain exception expected", signals.get(0).getThrowable().getMessage(),
				is("foo"));
	}

	@Test(expected = NullPointerException.class)
	public void testDoOnEachSignalNullConsumer() {
		Mono.just(1).doOnEach(null);
	}

	@Test
	public void testDoOnEachSignalToSubscriber() {
		AssertSubscriber<Integer> peekSubscriber = AssertSubscriber.create();
		Mono<Integer> mono = Mono.just(1)
		                         .doOnEach(s -> s.accept(peekSubscriber));
		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		peekSubscriber.assertNotSubscribed();
		peekSubscriber.assertValues(1);
		peekSubscriber.assertComplete();
	}

	@Test
	public void testMonoThenManySupplier() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux<String> test = Mono.just(1).thenMany(Flux.defer(() -> Flux.just("A", "B")));

		test.subscribe(ts);
		ts.assertValues("A", "B");
		ts.assertComplete();
	}

	// test issue https://github.com/reactor/reactor/issues/485
	@Test
	public void promiseOnErrorHandlesExceptions() throws Exception {
		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);

		try {
			Mono.fromCallable(() -> {
				throw new RuntimeException("Some Exception");
			})
			    .subscribeOn(Schedulers.parallel())
			    .doOnError(t -> latch1.countDown())
			    .doOnSuccess(v -> latch2.countDown())
			    .block();
		}
		catch (RuntimeException re){

		}
		assertThat("Error latch was counted down", latch1.await(1, TimeUnit.SECONDS), is(true));
		assertThat("Complete latch was not counted down", latch2.getCount(), is(1L));
	}

	@Test
	public void promiseOnAfter() throws Exception {
		String h = Mono.fromCallable(() -> {
			Thread.sleep(400);
			return "hello";
		})
		               .subscribeOn(Schedulers.parallel())
		               .then(Mono.just("world"))
		               .block();
		assertThat("Alternate mono not seen", h, is("world"));
	}

	@Test
	public void promiseDelays() throws Exception {
		Tuple2<Long, String> h = Mono.delay(Duration.ofMillis(3000))
		                             .log("time1")
		                             .map(d -> "Spring wins")
		                             .or(Mono.delay(Duration.ofMillis(2000)).log("time2").map(d -> "Spring Reactive"))
		                             .then(t -> Mono.just(t+ " world"))
		                             .elapsed()
		                             .block();
		assertThat("Alternate mono not seen", h.getT2(), is("Spring Reactive world"));
		System.out.println(h.getT1());
	}

	@Test
	public void testMono() throws Exception {
		MonoProcessor<String> promise = MonoProcessor.create();
		promise.onNext("test");
		final CountDownLatch successCountDownLatch = new CountDownLatch(1);
		promise.subscribe(v -> successCountDownLatch.countDown());
		assertThat("Failed", successCountDownLatch.await(10, TimeUnit.SECONDS));
	}

	private static Mono<Integer> handle(String t) {
		return Mono.just(t.length());
	}

	@Test
	public void testMonoAndFunction() {
		StepVerifier.create(Mono.just("source")
		                        .and(t -> handle(t)))
		            .expectNextMatches(pair -> pair.getT1().equals("source") && pair.getT2() == 6)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testMonoAndFunctionEmpty() {
		StepVerifier.create(
				Mono.<String>empty().and(t -> handle(t)))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testMonoAndFunctionRightSideEmpty() {
		StepVerifier.create(
				Mono.just("foo").and(t -> Mono.empty()))
		            .expectComplete()
		            .verify();
	}
}

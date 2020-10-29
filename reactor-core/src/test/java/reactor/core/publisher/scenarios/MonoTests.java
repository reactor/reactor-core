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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * @author Stephane Maldini
 */
public class MonoTests {

	@Test
	public void errorContinueOnMonoReduction() {
		AtomicReference<Tuple2<Class, Object>> ref = new AtomicReference<>();
		StepVerifier.create(Flux.just(1, 0, 2)
		                        .map(v -> 100 / v)
		                        .reduce((a, b) -> a + b)
		                        .onErrorContinue(ArithmeticException.class, (t, v) -> ref.set(Tuples.of(t.getClass(), v))))
		            .expectNext(100 + 50)
		            .verifyComplete();

		Assertions.assertThat(ref).hasValue(Tuples.of(ArithmeticException.class, 0));
	}

	@Test
	public void discardLocalOrder() {
		List<String> discardOrder = Collections.synchronizedList(new ArrayList<>(2));

		StepVerifier.create(Mono.just(1)
		                        .hide() //hide both avoid the fuseable AND tryOnNext usage
		                        .filter(i -> i % 2 == 0)
		                        .doOnDiscard(Number.class, i -> discardOrder.add("FIRST"))
		                        .doOnDiscard(Integer.class, i -> discardOrder.add("SECOND"))
		)
		            .expectComplete()
		            .verify();

		Assertions.assertThat(discardOrder).containsExactly("FIRST", "SECOND");
	}

	@Test
	public void testDoOnEachSignal() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		Mono<Integer> mono = Mono.just(1)
		                         .doOnEach(signals::add);
		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectNext(1)
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));

		assertThat(signals).hasSize(2);
		assertThat(signals.get(0).get()).as("onNext").isEqualTo(1);
		assertThat(signals.get(1).isOnComplete()).as("onComplete expected").isTrue();
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

		assertThat(signals).hasSize(1);
		assertThat(signals.get(0).isOnComplete()).as("onComplete expected").isTrue();

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

		assertThat(signals).hasSize(1);
		assertThat(signals.get(0).isOnError()).as("onError expected").isTrue();
		assertThat(signals.get(0).getThrowable()).as("plain exception expected").hasMessage("foo");
	}

	@Test
	public void testDoOnEachSignalNullConsumer() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.just(1).doOnEach(null);
		});
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
		assertThat(latch1.await(1, TimeUnit.SECONDS)).as ("Error latch was counted down").isTrue();
		assertThat(latch2.getCount()).as("Complete latch was not counted down").isEqualTo(1L);
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
		assertThat(h).as("Alternate mono not seen").isEqualTo("world");
	}

	@Test
	public void promiseDelays() throws Exception {
		Tuple2<Long, String> h = Mono.delay(Duration.ofMillis(3000))
		                             .log("time1")
		                             .map(d -> "Spring wins")
		                             .or(Mono.delay(Duration.ofMillis(2000)).log("time2").map(d -> "Spring Reactive"))
		                             .flatMap(t -> Mono.just(t+ " world"))
		                             .elapsed()
		                             .block();
		assertThat(h.getT2()).as("Alternate mono not seen").isEqualTo("Spring Reactive world");
		System.out.println(h.getT1());
	}

	@Test
	public void testMono() throws Exception {
		Sinks.One<String> promise = Sinks.one();
		promise.emitValue("test", FAIL_FAST);
		final CountDownLatch successCountDownLatch = new CountDownLatch(1);
		promise.asMono().subscribe(v -> successCountDownLatch.countDown());
		assertThat(successCountDownLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	private static Mono<Integer> handle(String t) {
		return Mono.just(t.length());
	}

	@Test
	public void testMonoAndFunction() {
		StepVerifier.create(Mono.just("source")
		                        .zipWhen(t -> handle(t)))
		            .expectNextMatches(pair -> pair.getT1().equals("source") && pair.getT2() == 6)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testMonoAndFunctionEmpty() {
		StepVerifier.create(
				Mono.<String>empty().zipWhen(MonoTests::handle))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testMonoAndFunctionRightSideEmpty() {
		StepVerifier.create(
				Mono.just("foo").zipWhen(t -> Mono.empty()))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void fromFutureSupplier() {
		AtomicInteger source = new AtomicInteger();

		Supplier<CompletableFuture<Integer>> supplier = () -> CompletableFuture.completedFuture(source.incrementAndGet());
		Mono<Number> mono = Mono.fromFuture(supplier);

		Assertions.assertThat(source).hasValue(0);

		Assertions.assertThat(mono.block())
		          .isEqualTo(source.get())
		          .isEqualTo(1);

		Assertions.assertThat(mono.block())
		          .isEqualTo(source.get())
		          .isEqualTo(2);
	}

	@Test
	public void fromCompletionStageSupplier() {
		AtomicInteger source = new AtomicInteger();

		Supplier<CompletableFuture<Integer>> supplier = () -> CompletableFuture.completedFuture(source.incrementAndGet());
		Mono<Number> mono = Mono.fromCompletionStage(supplier);

		Assertions.assertThat(source).hasValue(0);

		Assertions.assertThat(mono.block())
		          .isEqualTo(source.get())
		          .isEqualTo(1);

		Assertions.assertThat(mono.block())
		          .isEqualTo(source.get())
		          .isEqualTo(2);
	}

	@Test
	public void monoCacheContextHistory() {
		AtomicInteger contextFillCount = new AtomicInteger();
		Mono<String> cached = Mono.deferContextual(Mono::just)
		                          .map(ctx -> ctx.getOrDefault("a", "BAD"))
		                          .cache()
		                          .contextWrite(ctx -> ctx.put("a", "GOOD" + contextFillCount.incrementAndGet()));

		//at first pass, the context is captured
		String cacheMiss = cached.block();
		Assertions.assertThat(cacheMiss).as("cacheMiss").isEqualTo("GOOD1");
		Assertions.assertThat(contextFillCount).as("cacheMiss").hasValue(1);

		//at second subscribe, the Context fill attempt is still done, but ultimately ignored since first context is cached
		String cacheHit = cached.block();
		Assertions.assertThat(cacheHit).as("cacheHit").isEqualTo("GOOD1"); //value from the cache
		Assertions.assertThat(contextFillCount).as("cacheHit").hasValue(2); //function was still invoked

		//at third subscribe, function is called for the 3rd time, but the context is still cached
		String cacheHit2 = cached.block();
		Assertions.assertThat(cacheHit2).as("cacheHit2").isEqualTo("GOOD1");
		Assertions.assertThat(contextFillCount).as("cacheHit2").hasValue(3);

		//at fourth subscribe, function is called for the 4th time, but the context is still cached
		String cacheHit3 = cached.block();
		Assertions.assertThat(cacheHit3).as("cacheHit3").isEqualTo("GOOD1");
		Assertions.assertThat(contextFillCount).as("cacheHit3").hasValue(4);
	}

	@Test
	public void monoFromMonoDoesntCallAssemblyHook() {
		final Mono<Integer> source = Mono.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.from(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoFromFluxWrappingMonoDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.from(Mono.just(1).hide());

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.from(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoFromCallableFluxCallsAssemblyHook() {
		final Flux<Integer> source = Flux.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.from(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void monoFromFluxCallsAssemblyHook() {
		final Flux<Integer> source = Flux.just(1).hide();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.from(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void monoFromPublisherCallsAssemblyHook() {
		final Publisher<Integer> source = TestPublisher.create();
		Assertions.assertThat(source).isNotInstanceOf(Flux.class); //smoke test this is a Publisher

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.from(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void monoFromDirectMonoDoesntCallAssemblyHook() {
		final Mono<Integer> source = Mono.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.fromDirect(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoFromDirectFluxWrappingMonoDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.from(Mono.just(1).hide());

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.fromDirect(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoFromDirectCallableFluxCallsAssemblyHook() {
		final Flux<Integer> source = Flux.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.fromDirect(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void monoFromDirectFluxCallsAssemblyHook() {
		final Flux<Integer> source = Flux.just(1).hide();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.fromDirect(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void monoFromDirectPublisherCallsAssemblyHook() {
		final Publisher<Integer> source = TestPublisher.create();
		Assertions.assertThat(source).isNotInstanceOf(Flux.class); //smoke test this is a Publisher

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Mono.fromDirect(source);
		Assertions.assertThat(wrappedCount).hasValue(1);
	}
}

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

package reactor.guide;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.assertj.core.api.Assertions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * Tests mirroring snippets from the reference documentation.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
public class GuideTests {

	@Test
	@SuppressWarnings("unchecked")
	public void introFutureHell() {
		CompletableFuture<List<String>> ids = ifhIds(); // <1>

		CompletableFuture<List<String>> result = ids.thenComposeAsync(l -> { // <2>
			Stream<CompletableFuture<String>> zip =
					l.stream().map(i -> { // <3>
						         CompletableFuture<String> nameTask = ifhName(i); // <4>
						         CompletableFuture<Integer> statTask = ifhStat(i); // <5>

						         return nameTask.thenCombineAsync(statTask, (name, stat) -> "Name " + name + " has stats " + stat); // <6>
					         });
			List<CompletableFuture<String>> combinationList = zip.collect(Collectors.toList()); // <7>
			CompletableFuture<String>[] combinationArray = combinationList.toArray(new CompletableFuture[combinationList.size()]);

			CompletableFuture<Void> allDone = CompletableFuture.allOf(combinationArray); // <8>
			return allDone.thenApply(v -> combinationList.stream()
			                                             .map(CompletableFuture::join) // <9>
			                                             .collect(Collectors.toList()));
		});

		List<String> results = result.join(); // <10>
		assertThat(results).contains(
						"Name NameJoe has stats 103",
						"Name NameBart has stats 104",
						"Name NameHenry has stats 105",
						"Name NameNicole has stats 106",
						"Name NameABSLAJNFOAJNFOANFANSF has stats 121");
	}

	@Test
	public void introFutureHellReactorVersion() {
		Flux<String> ids = ifhrIds(); // <1>

		Flux<String> combinations =
				ids.flatMap(id -> { // <2>
					Mono<String> nameTask = ifhrName(id); // <3>
					Mono<Integer> statTask = ifhrStat(id); // <4>

					return nameTask.zipWith(statTask, // <5>
							(name, stat) -> "Name " + name + " has stats " + stat);
				});

		Mono<List<String>> result = combinations.collectList(); // <6>

		List<String> results = result.block(); // <7>
		assertThat(results).containsExactly( // <8>
				"Name NameJoe has stats 103",
				"Name NameBart has stats 104",
				"Name NameHenry has stats 105",
				"Name NameNicole has stats 106",
				"Name NameABSLAJNFOAJNFOANFANSF has stats 121"
		);
	}

	private CompletableFuture<String> ifhName(String id) {
		CompletableFuture<String> f = new CompletableFuture<>();
		f.complete("Name" + id);
		return f;
	}

	private CompletableFuture<Integer> ifhStat(String id) {
		CompletableFuture<Integer> f = new CompletableFuture<>();
		f.complete(id.length() + 100);
		return f;
	}

	private CompletableFuture<List<String>> ifhIds() {
		CompletableFuture<List<String>> ids = new CompletableFuture<>();
		ids.complete(Arrays.asList("Joe", "Bart", "Henry", "Nicole", "ABSLAJNFOAJNFOANFANSF"));
		return ids;
	}

	private Flux<String> ifhrIds() {
		return Flux.just("Joe", "Bart", "Henry", "Nicole", "ABSLAJNFOAJNFOANFANSF");
	}

	private Mono<String> ifhrName(String id) {
		return Mono.just("Name" + id);
	}

	private Mono<Integer> ifhrStat(String id) {
		return Mono.just(id.length() + 100);
	}

	@Test
	public void advancedComposedNow() {
		Function<Flux<String>, Flux<String>> filterAndMap =
				f -> f.filter(color -> !color.equals("orange"))
				      .map(String::toUpperCase);

		Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		    .doOnNext(System.out::println)
		    .transform(filterAndMap)
		    .subscribe(d -> System.out.println("Subscriber to Transformed MapAndFilter: "+d));
	}

	@Test
	public void advancedComposedDefer() {
		AtomicInteger ai = new AtomicInteger();
		Function<Flux<String>, Flux<String>> filterAndMap = f -> {
			if (ai.incrementAndGet() == 1) {
				return f.filter(color -> !color.equals("orange"))
				        .map(String::toUpperCase);
			}
			return f.filter(color -> !color.equals("purple"))
			        .map(String::toUpperCase);
		};

		Flux<String> composedFlux =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .transformDeferred(filterAndMap);

		composedFlux.subscribe(d -> System.out.println("Subscriber 1 to Composed MapAndFilter :"+d));
		composedFlux.subscribe(d -> System.out.println("Subscriber 2 to Composed MapAndFilter: "+d));
	}

	@Test
	public void advancedCold() {
		Flux<String> source = Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		                          .map(String::toUpperCase);

		source.subscribe(d -> System.out.println("Subscriber 1: "+d));
		source.subscribe(d -> System.out.println("Subscriber 2: "+d));
	}

	@Test
	public void advancedHot() {
		Sinks.Many<String> hotSource = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<String> hotFlux = hotSource.asFlux().map(String::toUpperCase);

		hotFlux.subscribe(d -> System.out.println("Subscriber 1 to Hot Source: "+d));

		hotSource.emitNext("blue", FAIL_FAST); // <1>
		hotSource.tryEmitNext("green").orThrow(); // <2>

		hotFlux.subscribe(d -> System.out.println("Subscriber 2 to Hot Source: "+d));

		hotSource.emitNext("orange", FAIL_FAST);
		hotSource.emitNext("purple", FAIL_FAST);
		hotSource.emitComplete(FAIL_FAST);
	}

	@Test
	public void advancedConnectable() throws InterruptedException {
		Flux<Integer> source = Flux.range(1, 3)
		                           .doOnSubscribe(s -> System.out.println("subscribed to source"));

		ConnectableFlux<Integer> co = source.publish();

		co.subscribe(System.out::println, e -> {}, () -> {});
		co.subscribe(System.out::println, e -> {}, () -> {});

		System.out.println("done subscribing");
		Thread.sleep(500);
		System.out.println("will now connect");

		co.connect();
	}

	@Test
	public void advancedConnectableAutoConnect() throws InterruptedException {
		Flux<Integer> source = Flux.range(1, 3)
		                           .doOnSubscribe(s -> System.out.println("subscribed to source"));

		Flux<Integer> autoCo = source.publish().autoConnect(2);

		autoCo.subscribe(System.out::println, e -> {}, () -> {});
		System.out.println("subscribed first");
		Thread.sleep(500);
		System.out.println("subscribing second");
		autoCo.subscribe(System.out::println, e -> {}, () -> {});
	}

	@Test
	public void advancedBatchingGrouping() {
		StepVerifier.create(
				Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
				    .groupBy(i -> i % 2 == 0 ? "even" : "odd")
				    .concatMap(g -> g.defaultIfEmpty(-1) //if empty groups, show them
				                     .map(String::valueOf) //map to string
				                     .startWith(g.key())) //start with the group's key
		)
		            .expectNext("odd", "1", "3", "5", "11", "13")
		            .expectNext("even", "2", "4", "6", "12")
		            .verifyComplete();
	}

	@Test
	public void advancedBatchingWindowingSizeOverlap() {
		StepVerifier.create(
				Flux.range(1, 10)
				    .window(5, 3) //overlapping windows
				    .concatMap(g -> g.defaultIfEmpty(-1)) //show empty windows as -1
		)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectNext(4, 5, 6, 7, 8)
		            .expectNext(7, 8, 9, 10)
		            .expectNext(10)
	                .verifyComplete();
	}

	@Test
	public void advancedBatchingWindowing() {
		StepVerifier.create(
				Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
				    .windowWhile(i -> i % 2 == 0)
				    .concatMap(g -> g.defaultIfEmpty(-1))
		)
		            .expectNext(-1, -1, -1) //respectively triggered by odd 1 3 5
	                .expectNext(2, 4, 6) // triggered by 11
	                .expectNext(12) // triggered by 13
	                // however, no empty completion window is emitted (would contain extra matching elements)
	                .verifyComplete();
	}

	@Test
	public void advancedBatchingBufferingSizeOverlap() {
		StepVerifier.create(
				Flux.range(1, 10)
				    .buffer(5, 3) //overlapping buffers
		)
		            .expectNext(Arrays.asList(1, 2, 3, 4, 5))
		            .expectNext(Arrays.asList(4, 5, 6, 7, 8))
		            .expectNext(Arrays.asList(7, 8, 9, 10))
		            .expectNext(Collections.singletonList(10))
		            .verifyComplete();
	}

	@Test
	public void advancedBatchingBuffering() {
		StepVerifier.create(
				Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
				    .bufferWhile(i -> i % 2 == 0)
		)
	                .expectNext(Arrays.asList(2, 4, 6)) // triggered by 11
	                .expectNext(Collections.singletonList(12)) // triggered by 13
	                .verifyComplete();
	}

	@Test
	public void advancedParallelJustDivided() {
		Flux.range(1, 10)
	        .parallel(2) //<1>
	        .subscribe(i -> System.out.println(Thread.currentThread().getName() + " -> " + i));
	}

	@Test
	public void advancedParallelParallelized() {
		Flux.range(1, 10)
	        .parallel(2)
	        .runOn(Schedulers.parallel())
	        .subscribe(i -> System.out.println(Thread.currentThread().getName() + " -> " + i));
	}

	private Flux<String> someStringSource() {
		return Flux.just("foo", "bar", "baz").hide();
	}

	@Test
	public void baseSubscriberFineTuneBackpressure() {
		Flux<String> source = someStringSource();

		source.map(String::toUpperCase)
		      .subscribe(new BaseSubscriber<String>() { // <1>
			      @Override
			      protected void hookOnSubscribe(Subscription subscription) {
				      // <2>
				      request(1); // <3>
			      }

			      @Override
			      protected void hookOnNext(String value) {
				      request(1); // <4>
			      }

			      //<5>
		      });
	}

	private String doSomethingDangerous(long i) {
		if (i < 5)
			return String.valueOf(i);
		throw new IllegalArgumentException("boom" + i);
	}

	private String doSecondTransform(String i) {
		return "item" + i;
	}

	@Test
	public void errorHandlingOnError() {
		Flux<String> s = Flux.range(1, 10)
		                     .map(v -> doSomethingDangerous(v)) // <1>
		                     .map(v -> doSecondTransform(v)); // <2>
		s.subscribe(value -> System.out.println("RECEIVED " + value), // <3>
				error -> System.err.println("CAUGHT " + error) // <4>
		);

		StepVerifier.create(s)
	                .expectNext("item1")
	                .expectNext("item2")
	                .expectNext("item3")
	                .expectNext("item4")
	                .verifyErrorMessage("boom5");
	}

	@Test
	public void errorHandlingTryCatch() {
		try {
			for (int i = 1; i < 11; i++) {
				String v1 = doSomethingDangerous(i); // <1>
				String v2 = doSecondTransform(v1); // <2>
				System.out.println("RECEIVED " + v2);
			}
		} catch (Throwable t) {
			System.err.println("CAUGHT " + t); // <3>
		}
	}

	@Test
	public void errorHandlingReturn() {
		Flux<String> flux =
		Flux.just(10)
		    .map(this::doSomethingDangerous)
		    .onErrorReturn("RECOVERED");

		StepVerifier.create(flux)
	                .expectNext("RECOVERED")
	                .verifyComplete();
	}

	@Test
	public void errorHandlingReturnFilter() {
		Flux<String> flux =
		Flux.just(10)
		    .map(this::doSomethingDangerous)
		    .onErrorReturn(e -> e.getMessage().equals("boom10"), "recovered10");

		StepVerifier.create(flux)
	                .expectNext("recovered10")
	                .verifyComplete();

		flux =
		Flux.just(9)
		    .map(this::doSomethingDangerous)
		    .onErrorReturn(e -> e.getMessage().equals("boom10"), "recovered10");

		StepVerifier.create(flux)
	                .verifyErrorMessage("boom9");
	}

	private Flux<String> callExternalService(String key) {
		if (key.equals("key2"))
			return Flux.error(new IllegalStateException("boom"));
		if (key.startsWith("timeout"))
			return Flux.error(new TimeoutException());
		if (key.startsWith("unknown"))
			return Flux.error(new UnknownKeyException());
		return Flux.just(key.replace("key", "value"));
	}

	private Flux<String> getFromCache(String key) {
		return Flux.just("outdated" + key);
	}

	@Test
	public void errorHandlingOnErrorResume() {
		Flux<String> flux =
				Flux.just("key1", "key2")
				    .flatMap(k ->
						    callExternalService(k) // <1>
								    .onErrorResume(e -> getFromCache(k)) // <2>
				    );

		StepVerifier.create(flux)
	                .expectNext("value1", "outdatedkey2")
	                .verifyComplete();
	}

	private class UnknownKeyException extends RuntimeException {

	}

	private Flux<String> registerNewEntry(String key, String value) {
		return Flux.just(key + "=" + value);
	}

	@Test
	public void errorHandlingOnErrorResumeDependingOnError() {
		Flux<String> flux =
		Flux.just("timeout1", "unknown", "key2")
		    .flatMap(k ->
				    callExternalService(k)
						    .onErrorResume(error -> { // <1>
							    if (error instanceof TimeoutException) // <2>
								    return getFromCache(k);
							    else if (error instanceof UnknownKeyException) // <3>
								    return registerNewEntry(k, "DEFAULT");
							    else
								    return Flux.error(error); // <4>
						    })
		    );

		StepVerifier.create(flux)
	                .expectNext("outdatedtimeout1")
	                .expectNext("unknown=DEFAULT")
	                .verifyErrorMessage("boom");
	}

	private class BusinessException extends RuntimeException {

		public BusinessException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	@Test
	public void errorHandlingRethrow1() {
		Flux<String> flux =
		Flux.just("timeout1")
		    .flatMap(k -> callExternalService(k)
				    .onErrorResume(original -> Flux.error(
						    new BusinessException("oops, SLA exceeded", original))
				    )
		    );

		StepVerifier.create(flux)
		            .verifyErrorMatches(e -> e instanceof BusinessException &&
				            e.getMessage().equals("oops, SLA exceeded") &&
				            e.getCause() instanceof TimeoutException);
	}

	@Test
	public void errorHandlingRethrow2() {
		Flux<String> flux =
		Flux.just("timeout1")
		    .flatMap(k -> callExternalService(k)
				    .onErrorMap(original -> new BusinessException("oops, SLA exceeded", original))
		    );

		StepVerifier.create(flux)
		            .verifyErrorMatches(e -> e instanceof BusinessException &&
				            e.getMessage().equals("oops, SLA exceeded") &&
				            e.getCause() instanceof TimeoutException);
	}

	private void log(String s) {
		System.out.println(s);
	}

	@Test
	public void errorHandlingSideEffect() {
		LongAdder failureStat = new LongAdder();
		Flux<String> flux =
		Flux.just("unknown")
		    .flatMap(k -> callExternalService(k) // <1>
				    .doOnError(e -> {
				        failureStat.increment();
				        log("uh oh, falling back, service failed for key " + k); // <2>
				    })
		        .onErrorResume(e -> getFromCache(k)) // <3>
		    );

		StepVerifier.create(flux)
	                .expectNext("outdatedunknown")
	                .verifyComplete();

		assertThat(failureStat.intValue()).isEqualTo(1);
	}

	@Test
	public void errorHandlingUsing() {
		AtomicBoolean isDisposed = new AtomicBoolean();
		Disposable disposableInstance = new Disposable() {
			@Override
			public void dispose() {
				isDisposed.set(true); // <4>
			}

			@Override
			public String toString() {
				return "DISPOSABLE";
			}
		};

		Flux<String> flux =
		Flux.using(
				() -> disposableInstance, // <1>
				disposable -> Flux.just(disposable.toString()), // <2>
				Disposable::dispose // <3>
		);

		StepVerifier.create(flux)
	                .expectNext("DISPOSABLE")
	                .verifyComplete();

		assertThat(isDisposed.get()).isTrue();
	}

	@Test
	public void errorHandlingDoFinally() {
		LongAdder statsCancel = new LongAdder(); // <1>

		Flux<String> flux =
				Flux.just("foo", "bar")
				    .doFinally(type -> {
					    if (type == SignalType.CANCEL) // <2>
						    statsCancel.increment(); // <3>
				    })
				    .take(1); // <4>

		StepVerifier.create(flux)
	                .expectNext("foo")
	                .verifyComplete();

		assertThat(statsCancel.intValue()).isEqualTo(1);
	}

	@Test
	public void errorHandlingIntervalMillisNotContinued() throws InterruptedException {
		VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		VirtualTimeScheduler.set(virtualTimeScheduler);

		Flux<String> flux =
		Flux.interval(Duration.ofMillis(250))
		    .map(input -> {
			    if (input < 3) return "tick " + input;
			    throw new RuntimeException("boom");
		    })
		    .onErrorReturn("Uh oh");

		flux.subscribe(System.out::println);
		//Thread.sleep(2100); // <1>

		virtualTimeScheduler.advanceTimeBy(Duration.ofHours(1));

		StepVerifier.withVirtualTime(() -> flux, () -> virtualTimeScheduler, Long.MAX_VALUE)
	                .thenAwait(Duration.ofSeconds(3))
	                .expectNext("tick 0")
	                .expectNext("tick 1")
	                .expectNext("tick 2")
	                .expectNext("Uh oh")
	                .verifyComplete();
	}

	@Test
	public void errorHandlingIntervalMillisRetried() throws InterruptedException {
		VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		VirtualTimeScheduler.set(virtualTimeScheduler);

		Flux<Tuple2<Long,String>> flux =
		Flux.interval(Duration.ofMillis(250))
		    .map(input -> {
			    if (input < 3) return "tick " + input;
			    throw new RuntimeException("boom");
		    })
		    .retry(1)
		    .elapsed(); // <1>

		flux.subscribe(System.out::println, System.err::println); // <2>

		//Thread.sleep(2100); // <3>

		virtualTimeScheduler.advanceTimeBy(Duration.ofHours(1));

		StepVerifier.withVirtualTime(() -> flux, () -> virtualTimeScheduler, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT2().equals("tick 0"))
		            .expectNextMatches(t -> t.getT2().equals("tick 1"))
		            .expectNextMatches(t -> t.getT2().equals("tick 2"))
		            .expectNextMatches(t -> t.getT2().equals("tick 0"))
		            .expectNextMatches(t -> t.getT2().equals("tick 1"))
		            .expectNextMatches(t -> t.getT2().equals("tick 2"))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void errorHandlingRetryWhenApproximateRetry() {
		Flux<String> flux =
		Flux.<String>error(new IllegalArgumentException()) // <1>
				.doOnError(System.out::println) // <2>
				.retryWhen(Retry.from(companion -> // <3>
						companion.take(3))); // <4>

		StepVerifier.create(flux)
	                .verifyComplete();

		StepVerifier.create(Flux.<String>error(new IllegalArgumentException()).retry(3))
	                .verifyError();
	}

	@Test
	public void errorHandlingRetryWhenEquatesRetry() {
		AtomicInteger errorCount = new AtomicInteger();
		Flux<String> flux =
				Flux.<String>error(new IllegalArgumentException())
						.doOnError(e -> errorCount.incrementAndGet())
						.retryWhen(Retry.from(companion -> // <1>
								companion.map(rs -> { // <2>
									if (rs.totalRetries() < 3) return rs.totalRetries(); // <3>
									else throw Exceptions.propagate(rs.failure()); // <4>
								})
						));

		StepVerifier.create(flux)
	                .verifyError(IllegalArgumentException.class);

		AtomicInteger retryNErrorCount = new AtomicInteger();
		StepVerifier.create(Flux.<String>error(new IllegalArgumentException()).doOnError(e -> retryNErrorCount.incrementAndGet()).retry(3))
	                .verifyError();

		assertThat(errorCount).hasValue(retryNErrorCount.get());
	}

	@Test
	public void errorHandlingRetryBuilders() {
		Throwable exception = new IllegalStateException("boom");
		Flux<String> errorFlux = Flux.error(exception);

		errorFlux.retryWhen(Retry.max(3))
		         .as(StepVerifier::create)
		         .verifyErrorSatisfies(e -> assertThat(e)
				         .hasMessage("Retries exhausted: 3/3")
				         .hasCause(exception));

		errorFlux.retryWhen(Retry.max(3).filter(error -> error instanceof NullPointerException))
		         .as(StepVerifier::create)
		         .verifyErrorMessage("boom");
	}

	@Test
	public void errorHandlingRetryWhenExponential() {
		AtomicInteger errorCount = new AtomicInteger();
		Flux<String> flux =
				Flux.<String>error(new IllegalStateException("boom"))
						.doOnError(e -> { // <1>
							errorCount.incrementAndGet();
							System.out.println(e + " at " + LocalTime.now());
						})
						.retryWhen(Retry
								.backoff(3, Duration.ofMillis(100)).jitter(0d) // <2>
								.doAfterRetry(rs -> System.out.println("retried at " + LocalTime.now())) // <3>
								.onRetryExhaustedThrow((spec, rs) -> rs.failure()) // <4>
						);

		StepVerifier.create(flux)
		            .verifyErrorSatisfies(e -> Assertions
				            .assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom"));

		assertThat(errorCount).hasValue(4);
	}

	@Test
	public void errorHandlingRetryWhenTransient() {
		AtomicInteger errorCount = new AtomicInteger(); // <1>
		AtomicInteger transientHelper = new AtomicInteger();
		Flux<Integer> transientFlux = Flux.<Integer>generate(sink -> {
			int i = transientHelper.getAndIncrement();
			if (i == 10) { // <2>
				sink.next(i);
				sink.complete();
			}
			else if (i % 3 == 0) { // <3>
				sink.next(i);
			}
			else {
				sink.error(new IllegalStateException("Transient error at " + i)); // <4>
			}
		})
				.doOnError(e -> errorCount.incrementAndGet());

transientFlux.retryWhen(Retry.max(2).transientErrors(true))  // <5>
             .blockLast();
assertThat(errorCount).hasValue(6); // <6>

		transientHelper.set(0);
		transientFlux.retryWhen(Retry.max(2).transientErrors(true))
		             .as(StepVerifier::create)
		             .expectNext(0, 3, 6, 9, 10)
		             .verifyComplete();

		transientHelper.set(0);
		transientFlux.retryWhen(Retry.max(2))
		             .as(StepVerifier::create)
		             .expectNext(0, 3)
		             .verifyErrorMessage("Retries exhausted: 2/2");

	}

	public String convert(int i) throws IOException {
		if (i > 3) {
			throw new IOException("boom " + i);
		}
		return "OK " + i;
	}

	@Test
	public void errorHandlingPropagateUnwrap() {
		Flux<String> converted = Flux
				.range(1, 10)
				.map(i -> {
					try { return convert(i); }
					catch (IOException e) { throw Exceptions.propagate(e); }
				});

		converted.subscribe(
				v -> System.out.println("RECEIVED: " + v),
				e -> {
					if (Exceptions.unwrap(e) instanceof IOException) {
						System.out.println("Something bad happened with I/O");
					} else {
						System.out.println("Something bad happened");
					}
				}
		);

		StepVerifier.create(converted)
	                .expectNext("OK 1")
	                .expectNext("OK 2")
	                .expectNext("OK 3")
	                .verifyErrorMessage("boom 4");
	}

	@Test
	public void producingGenerate() {
		Flux<String> flux = Flux.generate(
				() -> 0, // <1>
				(state, sink) -> {
					sink.next("3 x " + state + " = " + 3*state); // <2>
					if (state == 10) sink.complete(); // <3>
					return state + 1; // <4>
				});

		StepVerifier.create(flux)
	                .expectNext("3 x 0 = 0")
	                .expectNext("3 x 1 = 3")
	                .expectNext("3 x 2 = 6")
	                .expectNext("3 x 3 = 9")
	                .expectNext("3 x 4 = 12")
	                .expectNext("3 x 5 = 15")
	                .expectNext("3 x 6 = 18")
	                .expectNext("3 x 7 = 21")
	                .expectNext("3 x 8 = 24")
	                .expectNext("3 x 9 = 27")
	                .expectNext("3 x 10 = 30")
	                .verifyComplete();
	}


	@Test
	public void producingGenerateMutableState() {
		Flux<String> flux = Flux.generate(
				AtomicLong::new, // <1>
				(state, sink) -> {
					long i = state.getAndIncrement(); // <2>
					sink.next("3 x " + i + " = " + 3*i);
					if (i == 10) sink.complete();
					return state; // <3>
				});

		StepVerifier.create(flux)
	                .expectNext("3 x 0 = 0")
	                .expectNext("3 x 1 = 3")
	                .expectNext("3 x 2 = 6")
	                .expectNext("3 x 3 = 9")
	                .expectNext("3 x 4 = 12")
	                .expectNext("3 x 5 = 15")
	                .expectNext("3 x 6 = 18")
	                .expectNext("3 x 7 = 21")
	                .expectNext("3 x 8 = 24")
	                .expectNext("3 x 9 = 27")
	                .expectNext("3 x 10 = 30")
	                .verifyComplete();
	}

	interface MyEventListener<T> {
		void onDataChunk(List<T> chunk);
		void processComplete();
	}

	interface MyEventProcessor {
		void register(MyEventListener<String> eventListener);
		void dataChunk(String... values);
		void processComplete();
	}

	private MyEventProcessor myEventProcessor = new MyEventProcessor() {

		private MyEventListener<String> eventListener;
		private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

		@Override
		public void register(MyEventListener<String> eventListener) {
			this.eventListener = eventListener;
		}

		@Override
		public void dataChunk(String... values) {
			executor.schedule(() -> eventListener.onDataChunk(Arrays.asList(values)),
					500, TimeUnit.MILLISECONDS);
		}

		@Override
		public void processComplete() {
			executor.schedule(() -> eventListener.processComplete(),
					500, TimeUnit.MILLISECONDS);
		}
	};

	@Test
	public void producingCreate() {
		Flux<String> bridge = Flux.create(sink -> {
			myEventProcessor.register( // <4>
					new MyEventListener<String>() { // <1>

						public void onDataChunk(List<String> chunk) {
							for(String s : chunk) {
								sink.next(s); // <2>
							}
						}

						public void processComplete() {
							sink.complete(); // <3>
						}
					});
		});

		StepVerifier.withVirtualTime(() -> bridge)
	                .expectSubscription()
	                .expectNoEvent(Duration.ofSeconds(10))
	                .then(() -> myEventProcessor.dataChunk("foo", "bar", "baz"))
	                .expectNext("foo", "bar", "baz")
	                .expectNoEvent(Duration.ofSeconds(10))
	                .then(() -> myEventProcessor.processComplete())
	                .verifyComplete();
	}

	public String alphabet(int letterNumber) {
		if (letterNumber < 1 || letterNumber > 26) {
			return null;
		}
		int letterIndexAscii = 'A' + letterNumber - 1;
		return "" + (char) letterIndexAscii;
	}

	@Test
	public void producingHandle() {
		Flux<String> alphabet = Flux.just(-1, 30, 13, 9, 20)
            .handle((i, sink) -> {
                String letter = alphabet(i); // <1>
                if (letter != null) // <2>
                        sink.next(letter); // <3>
            });

		alphabet.subscribe(System.out::println);

		StepVerifier.create(alphabet)
	                .expectNext("M", "I", "T")
	                .verifyComplete();
	}


	public Flux<String> processOrFallback(Mono<String> source, Publisher<String> fallback) {
		return source
				.flatMapMany(phrase -> Flux.fromArray(phrase.split("\\s+")))
				.switchIfEmpty(fallback);
	}

	@Test
	public void testSplitPathIsUsed() {
		StepVerifier.create(processOrFallback(Mono.just("just a  phrase with    tabs!"),
				Mono.just("EMPTY_PHRASE")))
		            .expectNext("just", "a", "phrase", "with", "tabs!")
		            .verifyComplete();
	}

	@Test
	public void testEmptyPathIsUsed() {
		StepVerifier.create(processOrFallback(Mono.empty(), Mono.just("EMPTY_PHRASE")))
		            .expectNext("EMPTY_PHRASE")
		            .verifyComplete();
	}

	private Mono<String> executeCommand(String command) {
		return Mono.just(command + " DONE");
	}

	public Mono<Void> processOrFallback(Mono<String> commandSource, Mono<Void> doWhenEmpty) {
		return commandSource
				.flatMap(command -> executeCommand(command).then()) // <1>
				.switchIfEmpty(doWhenEmpty); // <2>
	}

	@Test
	public void testCommandEmptyPathIsUsedBoilerplate() {
		AtomicBoolean wasInvoked = new AtomicBoolean();
		AtomicBoolean wasRequested = new AtomicBoolean();
		Mono<Void> testFallback = Mono.<Void>empty()
				.doOnSubscribe(s -> wasInvoked.set(true))
				.doOnRequest(l -> wasRequested.set(true));

		processOrFallback(Mono.empty(), testFallback).subscribe();

		assertThat(wasInvoked.get()).isTrue();
		assertThat(wasRequested.get()).isTrue();
	}

	@Test
	public void testCommandEmptyPathIsUsed() {
		PublisherProbe<Void> probe = PublisherProbe.empty(); // <1>

		StepVerifier.create(processOrFallback(Mono.empty(), probe.mono())) // <2>
		            .verifyComplete();

		probe.assertWasSubscribed(); //<3>
		probe.assertWasRequested(); //<4>
		probe.assertWasNotCancelled(); //<5>
	}

	//Note: the following static methods and fields are grouped here on purpose
	//as they all relate to the same section of the reference guide (activating debug mode).
	//some of these lines are copied verbatim in the reference guide, like the declaration of toDebug.

	private Flux<String> urls() {
		return Flux.range(1, 5)
		           .map(i -> "https://www.mysite.io/quote" + i);
	}

	private Flux<String> doRequest(String url) {
		return Flux.just("{\"quote\": \"inspiring quote from " + url + "\"}");
	}

	private Mono<String> scatterAndGather(Flux<String> urls) {
		return urls.flatMap(this::doRequest)
		           .single();
	}

	@BeforeEach
	public void populateDebug(TestInfo testInfo) {
		if (testInfo.getTags().contains("debugModeOn")) {
			Hooks.onOperatorDebug();
		}
		if (testInfo.getTags().contains("debugInit")) {
			toDebug = scatterAndGather(urls());
		}
	}

	@AfterEach
	public void removeHooks(TestInfo testInfo) {
		if (testInfo.getTags().contains("debugModeOn")) {
			Hooks.resetOnOperatorDebug();
		}
	}

	public Mono<String> toDebug; //please overlook the public class attribute :p

	private void printAndAssert(Throwable t, boolean checkForAssemblySuppressed) {
		t.printStackTrace();
		assertThat(t)
				.isInstanceOf(IndexOutOfBoundsException.class)
				.hasMessage("Source emitted more than one item");
		if (!checkForAssemblySuppressed) {
			assertThat(t).hasNoSuppressedExceptions();
		}
		else {
			assertThat(t).satisfies(withSuppressed -> {
				assertThat(withSuppressed.getSuppressed()).hasSize(1);
				assertThat(withSuppressed.getSuppressed()[0])
						.hasMessageStartingWith("\nAssembly trace from producer [reactor.core.publisher.MonoSingle] :")
						.hasMessageContaining("Flux.single ⇢ at reactor.guide.GuideTests.scatterAndGather(GuideTests.java:1017)\n");
			});
		}
	}

	@Test
	@Tag("debugInit")
	public void debuggingCommonStacktrace() {
		toDebug.subscribe(System.out::println, t -> printAndAssert(t, false));
	}

	@Test
	@Tag("debugModeOn")
	@Tag("debugInit")
	public void debuggingActivated() {
		toDebug.subscribe(System.out::println, t -> printAndAssert(t, true));
	}

	@Test
	public void debuggingLogging() {
		Flux<Integer> flux = Flux.range(1, 10)
		                         .log()
		                         .take(3);
		//flux.subscribe();

		//nothing much to test, but...
		StepVerifier.create(flux).expectNext(1, 2, 3).verifyComplete();
	}

	@Test
	public void contextSimple1() {
		//use of two-space indentation on purpose to maximise readability in refguide
String key = "message";
Mono<String> r = Mono.just("Hello")
    .flatMap(s -> Mono.deferContextual(ctx ->
         Mono.just(s + " " + ctx.get(key)))) //<2>
    .contextWrite(ctx -> ctx.put(key, "World")); //<1>

StepVerifier.create(r)
            .expectNext("Hello World") //<3>
            .verifyComplete();
	}

	@Test
	public void contextSimple2() {
		//use of two-space indentation on purpose to maximise readability in refguide
String key = "message";
Mono<String> r = Mono.just("Hello")
    .contextWrite(ctx -> ctx.put(key, "World")) //<1>
    .flatMap( s -> Mono.deferContextual(ctx ->
        Mono.just(s + " " + ctx.getOrDefault(key, "Stranger")))); //<2>

StepVerifier.create(r)
            .expectNext("Hello Stranger") //<3>
            .verifyComplete();
	}

	//contextSimple3 deleted since writes are not exposed anymore with ContextView

	@Test
	public void contextSimple4() {
		//use of two-space indentation on purpose to maximise readability in refguide
String key = "message";
Mono<String> r = Mono
    .deferContextual(ctx -> Mono.just("Hello " + ctx.get(key)))
    .contextWrite(ctx -> ctx.put(key, "Reactor")) //<1>
    .contextWrite(ctx -> ctx.put(key, "World")); //<2>

StepVerifier.create(r)
            .expectNext("Hello Reactor") //<3>
            .verifyComplete();
	}

	@Test
	public void contextSimple5() {
		//use of two-space indentation on purpose to maximise readability in refguide
String key = "message";
Mono<String> r = Mono
    .deferContextual(ctx -> Mono.just("Hello " + ctx.get(key))) //<3>
    .contextWrite(ctx -> ctx.put(key, "Reactor")) //<2>
    .flatMap( s -> Mono.deferContextual(ctx ->
        Mono.just(s + " " + ctx.get(key)))) //<4>
    .contextWrite(ctx -> ctx.put(key, "World")); //<1>

StepVerifier.create(r)
            .expectNext("Hello Reactor World") //<5>
            .verifyComplete();
	}

	@Test
	public void contextSimple6() {
		//use of two-space indentation on purpose to maximise readability in refguide
String key = "message";
Mono<String> r = Mono.just("Hello")
    .flatMap( s -> Mono
        .deferContextual(ctxView -> Mono.just(s + " " + ctxView.get(key)))
    )
    .flatMap( s -> Mono
        .deferContextual(ctxView -> Mono.just(s + " " + ctxView.get(key)))
        .contextWrite(ctx -> ctx.put(key, "Reactor")) //<1>
    )
    .contextWrite(ctx -> ctx.put(key, "World")); // <2>

StepVerifier.create(r)
            .expectNext("Hello World Reactor")
            .verifyComplete();
	}

//use of two-space indentation on purpose to maximise readability in refguide
static final String HTTP_CORRELATION_ID = "reactive.http.library.correlationId";

Mono<Tuple2<Integer, String>> doPut(String url, Mono<String> data) {
  Mono<Tuple2<String, Optional<Object>>> dataAndContext =
      data.zipWith(Mono.deferContextual(c -> // <1>
          Mono.just(c.getOrEmpty(HTTP_CORRELATION_ID))) // <2>
      );

  return dataAndContext.<String>handle((dac, sink) -> {
      if (dac.getT2().isPresent()) { // <3>
        sink.next("PUT <" + dac.getT1() + "> sent to " + url +
            " with header X-Correlation-ID = " + dac.getT2().get());
      }
      else {
        sink.next("PUT <" + dac.getT1() + "> sent to " + url);
      }
        sink.complete();
      })
      .map(msg -> Tuples.of(200, msg));
}

//use of two-space indentation on purpose to maximise readability in refguide
@Test
public void contextForLibraryReactivePut() {
  Mono<String> put = doPut("www.example.com", Mono.just("Walter"))
      .contextWrite(Context.of(HTTP_CORRELATION_ID, "2-j3r9afaf92j-afkaf"))
      .filter(t -> t.getT1() < 300)
      .map(Tuple2::getT2);

  StepVerifier.create(put)
              .expectNext("PUT <Walter> sent to www.example.com" +
                  " with header X-Correlation-ID = 2-j3r9afaf92j-afkaf")
              .verifyComplete();
}

	@Test
	public void contextForLibraryReactivePutNoContext() {
		//use of two-space indentation on purpose to maximise readability in refguide
Mono<String> put = doPut("www.example.com", Mono.just("Walter"))
    .filter(t -> t.getT1() < 300)
    .map(Tuple2::getT2);

StepVerifier.create(put)
            .expectNext("PUT <Walter> sent to www.example.com")
            .verifyComplete();
	}
}

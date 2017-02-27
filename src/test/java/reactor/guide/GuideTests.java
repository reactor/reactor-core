/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests mirroring snippets from the reference documentation.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */

public class GuideTests {

	@Test
	public void fluxComposing() throws Exception {
		Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		    .log()
		    .filter(color -> !color.equals("orange"))
		    .map(String::toUpperCase)
		    .subscribe(d -> System.out.println("Subscriber to Map: "+d));

		System.out.println("\n");

		Flux<String> flux =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .filter(color -> !color.equals("orange"));

		flux.map(String::toUpperCase);
		flux.subscribe(d -> System.out.println("Subscriber to Filter: "+d));

		System.out.println("\n");

		Flux<String> source =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .filter(color -> !color.equals("orange"))
				    .map(String::toUpperCase);

		source.subscribe(d -> System.out.println("Subscriber 1: "+d));
		source.subscribe(d -> System.out.println("Subscriber 2: "+d));

		System.out.println("\n");

		Function<Flux<String>, Flux<String>> filterAndMap =
				f -> f.filter(color -> !color.equals("orange"))
				      .map(String::toUpperCase);

		Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		    .doOnNext(System.out::println)
		    .transform(filterAndMap)
		    .subscribe(d -> System.out.println("Subscriber to Transformed MapAndFilter: "+d));

		System.out.println("\n");

		AtomicInteger ai = new AtomicInteger();
		filterAndMap = f -> {
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
				    .compose(filterAndMap);

		composedFlux.subscribe(d -> System.out.println("Subscriber 1 to Composed MapAndFilter :"+d));
		composedFlux.subscribe(d -> System.out.println("Subscriber 2 to Composed MapAndFilter: "+d));

		System.out.println("\n");

		UnicastProcessor<String> hotSource = UnicastProcessor.create();

		Flux<String> hotFlux = hotSource.doOnNext(System.out::println)
		                                .publish()
		                                .autoConnect();

		hotFlux.subscribe(d -> System.out.println("Subscriber 1 to Hot Source: "+d));

		hotSource.onNext("blue");
		hotSource.onNext("green");

		hotFlux.subscribe(d -> System.out.println("Subscriber 2 to Hot Source: "+d));

		hotSource.onNext("orange");
		hotSource.onNext("purple");
		hotSource.onComplete();
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
	public void errorHandlingSwitchOnError() {
		Flux<String> flux =
				Flux.just("key1", "key2")
				    .flatMap(k ->
						    callExternalService(k) // <1>
								    .switchOnError(getFromCache(k)) // <2>
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
	public void errorHandlingResumeWith() {
		Flux<String> flux =
		Flux.just("timeout1", "unknown", "key2")
		    .flatMap(k ->
				    callExternalService(k)
						    .onErrorResumeWith(error -> { // <1>
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
	public void errorHandlingRethrow() {
		Flux<String> flux =
		Flux.just("timeout1")
		    .flatMap(k -> callExternalService(k)
				    .onErrorResumeWith(original -> Flux.error(
						    new BusinessException("oops, SLA exceeded", original))
				    )
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
		        .switchOnError(getFromCache(k)) // <3>
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
		VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.createForAll();
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
		VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.createForAll();
		VirtualTimeScheduler.set(virtualTimeScheduler);

		Flux<Tuple2<Long,String>> flux =
		Flux.interval(Duration.ofMillis(250))
		    .map(input -> {
			    if (input < 3) return "tick " + input;
			    throw new RuntimeException("boom");
		    })
		    .elapsed() // <1>
		    .retry(1);

		flux.subscribe(System.out::println,
				System.err::println); // <2>

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
				.retryWhen(companion -> companion.take(3)); // <3>

		StepVerifier.create(flux)
	                .verifyComplete();

		StepVerifier.create(Flux.<String>error(new IllegalArgumentException()).retry(3))
	                .verifyError();
	}

	@Test
	public void errorHandlingRetryWhenEquatesRetry() {
		Flux<String> flux =
		Flux.<String>error(new IllegalArgumentException())
				.retryWhen(companion -> companion
						.zipWith(Flux.range(1, 4), (error, index) -> { // <1>
							if (index < 4) return index; // <2>
							else throw Exceptions.propagate(error); // <3>
						})
				);

		StepVerifier.create(flux)
	                .verifyError(IllegalArgumentException.class);

		StepVerifier.create(Flux.<String>error(new IllegalArgumentException()).retry(3))
	                .verifyError();
	}

	@Test
	public void errorHandlingRetryWhenExponential() {
		Flux<String> flux =
		Flux.<String>error(new IllegalArgumentException())
				.retryWhen(companion -> companion
						.doOnNext(s -> System.out.println(s + " at " + LocalTime.now())) // <1>
						.zipWith(Flux.range(1, 4), (error, index) -> { // <2>
							if (index < 4) return index;
							else throw Exceptions.propagate(error);
						})
						.flatMap(index -> Mono.delay(Duration.ofMillis(index * 100))) // <3>
						.doOnNext(s -> System.out.println("retried at " + LocalTime.now())) // <4>
				);

		StepVerifier.create(flux)
		            .verifyError(IllegalArgumentException.class);
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

	private Flux<String> urls() {
		return Flux.range(1, 5)
		           .map(i -> "http://mysite.io/quote/" + i);
	}

	private Flux<String> doRequest(String url) {
		return Flux.just("{\"quote\": \"inspiring quote from " + url + "\"}");
	}

	private Mono<String> scatterAndGather(Flux<String> urls) {
		return urls.flatMap(url -> doRequest(url))
		           .single();
	}

	@Rule
	public TestName testName = new TestName();

	@Before
	public void populateDebug() {
		if (testName.getMethodName().equals("debuggingCommonStacktrace")) {
			toDebug = scatterAndGather(urls());
		}
		else if (testName.getMethodName().equals("debuggingActivatedForSpecific")) {
			Hooks.onOperator(hook -> hook
					.ifNameContains("single")
					.operatorStacktrace());
			toDebug = scatterAndGather(urls());
		}
		else if (testName.getMethodName().startsWith("debuggingActivated")) {
			Hooks.onOperator(Hooks.OperatorHook::operatorStacktrace);
			toDebug = scatterAndGather(urls());
		}
	}

	@After
	public void removeHooks() {
		if (testName.getMethodName().startsWith("debuggingActivated")) {
			Hooks.resetOnOperator();
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
						.hasMessageEndingWith("Flux.single(GuideTests.java:685)\n");
			});
		}
	}

	@Test
	public void debuggingCommonStacktrace() {
		toDebug.subscribe(System.out::println, t -> printAndAssert(t, false));
	}

	@Test
	public void debuggingActivated() {
		toDebug.subscribe(System.out::println, t -> printAndAssert(t, true));
	}

	@Test
	public void debuggingActivatedForSpecific() {
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
}

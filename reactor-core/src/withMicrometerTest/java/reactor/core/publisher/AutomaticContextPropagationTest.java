/*
 * Copyright (c) 2023-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ThreadLocalAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.TestSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class AutomaticContextPropagationTest {

	private static final String KEY = "ContextPropagationTest.key";
	private static final ThreadLocal<String> REF = ThreadLocal.withInitial(() -> "ref_init");

	@BeforeAll
	static void initializeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.registerThreadLocalAccessor(KEY, REF);
	}

	@BeforeEach
	void enableAutomaticContextPropagation() {
		Hooks.enableAutomaticContextPropagation();
		// Disabling is done by ReactorTestExecutionListener
	}

	@AfterEach
	void cleanupThreadLocals() {
		REF.remove();
	}

	@AfterAll
	static void removeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.removeThreadLocalAccessor(KEY);
	}

	@Test
	void threadLocalsPresentAfterSubscribeOn() {
		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .subscribeOn(Schedulers.boundedElastic())
		    .doOnNext(i -> tlValue.set(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentAfterPublishOn() {
		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .publishOn(Schedulers.boundedElastic())
		    .doOnNext(i -> tlValue.set(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentInFlatMap() {
		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .flatMap(i -> Mono.just(i)
		                      .doOnNext(j -> tlValue.set(REF.get())))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentAfterDelay() {
		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .delayElements(Duration.ofMillis(1))
		    .doOnNext(i -> tlValue.set(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentInDoOnSubscribe() {
		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
			.subscribeOn(Schedulers.boundedElastic())
			.doOnSubscribe(s -> tlValue.set(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentInDoOnEach() {
		ArrayBlockingQueue<String> threadLocals = new ArrayBlockingQueue<>(4);
		Flux.just(1, 2, 3)
		    .doOnEach(s -> threadLocals.add(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(threadLocals).containsOnly("present", "present", "present", "present");
	}

	@Test
	void threadLocalsPresentInDoOnRequest() {
		AtomicReference<String> tlValue1 = new AtomicReference<>();
		AtomicReference<String> tlValue2 = new AtomicReference<>();

		Flux.just(1)
		    .subscribeOn(Schedulers.boundedElastic())
		    .doOnRequest(s -> tlValue1.set(REF.get()))
		    .publishOn(Schedulers.single())
		    .doOnRequest(s -> tlValue2.set(REF.get()))
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		assertThat(tlValue1.get()).isEqualTo("present");
		assertThat(tlValue2.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentInDoAfterTerminate() throws InterruptedException, TimeoutException {
		AtomicReference<String> tlValue = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);

		Flux.just(1)
		    .subscribeOn(Schedulers.boundedElastic())
		    .doAfterTerminate(() -> {
				tlValue.set(REF.get());
				latch.countDown();
		    })
		    .contextWrite(Context.of(KEY, "present"))
		    .blockLast();

		// Need to synchronize, as the doAfterTerminate operator can race with the
		// assertion. First, blockLast receives the completion signal, and only then,
		// the callback is triggered.
		if (!latch.await(100, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException("timed out");
		}
		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void contextCapturePropagatedAutomaticallyToAllSignals() throws InterruptedException {
		AtomicReference<String> requestTlValue = new AtomicReference<>();
		AtomicReference<String> subscribeTlValue = new AtomicReference<>();
		AtomicReference<String> firstNextTlValue = new AtomicReference<>();
		AtomicReference<String> secondNextTlValue = new AtomicReference<>();
		AtomicReference<String> cancelTlValue = new AtomicReference<>();

		CountDownLatch itemDelivered = new CountDownLatch(1);
		CountDownLatch cancelled = new CountDownLatch(1);

		TestSubscriber<Integer> subscriber =
				TestSubscriber.builder().initialRequest(1).build();

		REF.set("downstreamContext");

		Flux.just(1, 2, 3)
		    .hide()
		    .doOnRequest(r -> requestTlValue.set(REF.get()))
		    .doOnNext(i -> firstNextTlValue.set(REF.get()))
		    .doOnSubscribe(s -> subscribeTlValue.set(REF.get()))
		    .doOnCancel(() -> {
			    cancelTlValue.set(REF.get());
			    cancelled.countDown();
		    })
		    .delayElements(Duration.ofMillis(1))
		    .contextWrite(Context.of(KEY, "upstreamContext"))
		    // disabling prefetching to observe cancellation
		    .publishOn(Schedulers.parallel(), 1)
		    .doOnNext(i -> {
			    secondNextTlValue.set(REF.get());
			    itemDelivered.countDown();
		    })
		    .subscribeOn(Schedulers.boundedElastic())
		    .contextCapture()
		    .subscribe(subscriber);

		itemDelivered.await();

		subscriber.cancel();

		cancelled.await();

		assertThat(requestTlValue.get()).isEqualTo("upstreamContext");
		assertThat(subscribeTlValue.get()).isEqualTo("upstreamContext");
		assertThat(firstNextTlValue.get()).isEqualTo("upstreamContext");
		assertThat(cancelTlValue.get()).isEqualTo("upstreamContext");
		assertThat(secondNextTlValue.get()).isEqualTo("downstreamContext");
	}

	@Test
	void prefetchingShouldMaintainThreadLocals() {
		// We validate streams of items above default prefetch size
		// (max concurrency of flatMap == Queues.SMALL_BUFFER_SIZE == 256)
		// are able to maintain the context propagation to ThreadLocals
		// in the presence of prefetching
		int size = Queues.SMALL_BUFFER_SIZE * 10;

		Flux<Integer> source = Flux.create(s -> {
			for (int i = 0; i < size; i++) {
				s.next(i);
			}
			s.complete();
		});

		assertThat(REF.get()).isEqualTo("ref_init");

		ArrayBlockingQueue<String> innerThreadLocals = new ArrayBlockingQueue<>(size);
		ArrayBlockingQueue<String> outerThreadLocals = new ArrayBlockingQueue<>(size);

		source.publishOn(Schedulers.boundedElastic())
		      .flatMap(i -> Mono.just(i)
		                        .delayElement(Duration.ofMillis(1))
		                        .doOnNext(j -> innerThreadLocals.add(REF.get())))
		      .contextWrite(ctx -> ctx.put(KEY, "present"))
		      .publishOn(Schedulers.parallel())
		      .doOnNext(i -> outerThreadLocals.add(REF.get()))
		      .blockLast();

		assertThat(innerThreadLocals).containsOnly("present").hasSize(size);
		assertThat(outerThreadLocals).containsOnly("ref_init").hasSize(size);
	}

	@Test
	void fluxApiUsesContextPropagationConstantFunction() {
		Flux<Integer> source = Flux.empty();
		assertThat(source.contextCapture())
				.isInstanceOfSatisfying(FluxContextWriteRestoringThreadLocals.class,
						fcw -> assertThat(fcw.doOnContext)
								.as("flux's capture function")
								.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
				);
	}

	@Test
	void monoApiUsesContextPropagationConstantFunction() {
		Mono<Integer> source = Mono.empty();
		assertThat(source.contextCapture())
				.isInstanceOfSatisfying(MonoContextWriteRestoringThreadLocals.class,
						fcw -> assertThat(fcw.doOnContext)
								.as("mono's capture function")
								.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE));
	}

	@Nested
	class NonReactorFluxOrMono {

		private ExecutorService executorService;

		@BeforeEach
		void enableAutomaticContextPropagation() {
			executorService = Executors.newSingleThreadExecutor();
		}

		@AfterEach
		void cleanupThreadLocals() {
			executorService.shutdownNow();
		}

		// Scaffold methods

		private ThreadSwitchingFlux<String> threadSwitchingFlux() {
			return new ThreadSwitchingFlux<>("Hello", executorService);
		}

		private ThreadSwitchingMono<String> threadSwitchingMono() {
			return new ThreadSwitchingMono<>("Hello", executorService);
		}

		private ThreadSwitchingConnectableFlux<String> threadSwitchingConnectableFlux() {
			return new ThreadSwitchingConnectableFlux<>("Hello", executorService);
		}

		void assertThreadLocalsPresentInFlux(Supplier<Flux<?>> chainSupplier) {
			assertThreadLocalsPresentInFlux(chainSupplier, false);
		}

		void assertThreadLocalsPresentInFlux(Supplier<Flux<?>> chainSupplier,
				boolean skipCoreSubscriber) {
			assertThreadLocalsPresent(chainSupplier.get());
			assertThatThreadLocalsPresentDirectRawSubscribe(chainSupplier.get());
			if (!skipCoreSubscriber) {
				assertThatThreadLocalsPresentDirectCoreSubscribe(chainSupplier.get());
			}
		}

		void assertThreadLocalsPresentInMono(Supplier<Mono<?>> chainSupplier) {
			assertThreadLocalsPresentInMono(chainSupplier, false);
		}

		void assertThreadLocalsPresentInMono(Supplier<Mono<?>> chainSupplier,
				boolean skipCoreSubscriber) {
			assertThreadLocalsPresent(chainSupplier.get());
			assertThatThreadLocalsPresentDirectRawSubscribe(chainSupplier.get());
			if (!skipCoreSubscriber) {
				assertThatThreadLocalsPresentDirectCoreSubscribe(chainSupplier.get());
			}
		}

		void assertThreadLocalsPresent(Flux<?> chain) {
			AtomicReference<String> tlInOnNext = new AtomicReference<>();
			AtomicReference<String> tlInOnComplete = new AtomicReference<>();
			AtomicReference<String> tlInOnError = new AtomicReference<>();

			AtomicBoolean hadNext = new AtomicBoolean(false);
			AtomicReference<Throwable> error = new AtomicReference<>();

			try {
				chain.doOnEach(signal -> {
					     if (signal.isOnNext()) {
						     tlInOnNext.set(REF.get());
						     hadNext.set(true);
					     }
					     else if (signal.isOnError()) {
						     tlInOnError.set(REF.get());
						     error.set(signal.getThrowable());
					     }
					     else if (signal.isOnComplete()) {
						     tlInOnComplete.set(REF.get());
					     }
				     })
				     .contextWrite(Context.of(KEY, "present"))
				     .blockLast(Duration.ofMillis(5000));
			} catch (Exception e) {
				if (!(e instanceof ExpectedException || e.getCause() instanceof TimeoutException)) {
					throw e;
				}
				assertThat(e).satisfiesAnyOf(
						exception -> assertThat(exception).isEqualTo(error.get()),
						exception -> assertThat(exception).hasCause(error.get()));
			}

			if (hadNext.get()) {
				assertThat(tlInOnNext.get()).isEqualTo("present");
			}
			if (error.get() != null) {
				assertThat(tlInOnError.get()).isEqualTo("present");
			} else {
				assertThat(tlInOnComplete.get()).isEqualTo("present");
			}
		}

		void assertThreadLocalsPresent(Mono<?> chain) {
			AtomicReference<String> tlInOnNext = new AtomicReference<>();
			AtomicReference<String> tlInOnComplete = new AtomicReference<>();
			AtomicReference<String> tlInOnError = new AtomicReference<>();

			AtomicBoolean hadNext = new AtomicBoolean(false);
			AtomicBoolean hadError = new AtomicBoolean(false);

			chain.doOnEach(signal -> {
				     if (signal.isOnNext()) {
					     tlInOnNext.set(REF.get());
					     hadNext.set(true);
				     }
				     else if (signal.isOnError()) {
					     tlInOnError.set(REF.get());
					     hadError.set(true);
				     }
				     else if (signal.isOnComplete()) {
					     tlInOnComplete.set(REF.get());
				     }
			     })
			     .contextWrite(Context.of(KEY, "present"))
			     .onErrorComplete(e -> e instanceof ExpectedException || e instanceof TimeoutException)
			     .block();

			if (hadNext.get()) {
				assertThat(tlInOnNext.get()).isEqualTo("present");
			}
			if (hadError.get()) {
				assertThat(tlInOnError.get()).isEqualTo("present");
			} else {
				assertThat(tlInOnComplete.get()).isEqualTo("present");
			}
		}

		<T> void assertThatThreadLocalsPresentDirectCoreSubscribe(
				CorePublisher<? extends T> source) {
			assertThatThreadLocalsPresentDirectCoreSubscribe(source, () -> {});
		}

		<T> void assertThatThreadLocalsPresentDirectCoreSubscribe(
				CorePublisher<? extends T> source, Runnable asyncAction) {
			assertThatNoException().isThrownBy(() -> {
				CoreSubscriberWithContext<T> subscriberWithContext = new CoreSubscriberWithContext<>();

				source.subscribe(subscriberWithContext);

				executorService.submit(asyncAction)
				               .get(100, TimeUnit.MILLISECONDS);

				if (!subscriberWithContext.latch.await(2, TimeUnit.SECONDS)) {
					throw new TimeoutException("timed out");
				}

				if (subscriberWithContext.hadNext.get()) {
					assertThat(subscriberWithContext.valueInOnNext.get()).isEqualTo(
							"present");
				}
				if (subscriberWithContext.error.get() == null) {
					assertThat(subscriberWithContext.valueInOnComplete.get()).isEqualTo("present");
					assertThat(subscriberWithContext.complete).isTrue();
				}
				else {
					assertThat(subscriberWithContext.error.get()).isInstanceOfAny(ExpectedException.class, TimeoutException.class);
					assertThat(subscriberWithContext.valueInOnError.get()).isEqualTo("present");
				}
			});
		}

		// We force the use of subscribe(Subscriber) override instead of
		// subscribe(CoreSubscriber), and we can observe that for such a case we
		// are able to wrap the Subscriber and restore ThreadLocal values for the
		// signals received downstream.
		<T> void assertThatThreadLocalsPresentDirectRawSubscribe(
				Publisher<? extends T> source) {
			assertThatThreadLocalsPresentDirectRawSubscribe(source, () -> {});
		}

		<T> void assertThatThreadLocalsPresentDirectRawSubscribe(
				Publisher<? extends T> source, Runnable asyncAction) {
			assertThatNoException().isThrownBy(() -> {
				CoreSubscriberWithContext<T> subscriberWithContext = new CoreSubscriberWithContext<>();

				source.subscribe(subscriberWithContext);

				executorService.submit(asyncAction)
				               .get(100, TimeUnit.MILLISECONDS);

				if (!subscriberWithContext.latch.await(2, TimeUnit.SECONDS)) {
					throw new TimeoutException("timed out");
				}

				if (subscriberWithContext.hadNext.get()) {
					assertThat(subscriberWithContext.valueInOnNext.get()).isEqualTo("present");
				}
				if (subscriberWithContext.error.get() == null) {
					assertThat(subscriberWithContext.valueInOnComplete.get()).isEqualTo("present");
					assertThat(subscriberWithContext.complete).isTrue();
				}
				else {
					assertThat(subscriberWithContext.error.get()).isInstanceOfAny(ExpectedException.class, TimeoutException.class);
					assertThat(subscriberWithContext.valueInOnError.get()).isEqualTo("present");
				}
			});
		}

		// Fundamental tests for Flux

		@Test
		void fluxSubscribe() {
			assertThreadLocalsPresentInFlux(this::threadSwitchingFlux, true);
		}

		@Test
		void internalFluxFlatMapSubscribe() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello")
					    .flatMap(item -> threadSwitchingFlux()));
		}

		@Test
		void internalFluxSubscribeNoFusion() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .flatMap(item -> threadSwitchingFlux()));
		}

		@Test
		void directFluxSubscribeAsCoreSubscriber() throws InterruptedException, TimeoutException {
			Flux<String> flux = threadSwitchingFlux();

			CoreSubscriberWithContext<String> subscriberWithContext = new CoreSubscriberWithContext<>();

			flux.subscribe(subscriberWithContext);

			if (!subscriberWithContext.latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(subscriberWithContext.error.get()).isNull();
			assertThat(subscriberWithContext.complete.get()).isTrue();

			// We can't do anything here. subscribe(CoreSubscriber) is abstract in
			// CoreSubscriber interface and we have no means to intercept the calls to
			// restore ThreadLocals.
			assertThat(subscriberWithContext.valueInOnNext.get()).isEqualTo("ref_init");
			assertThat(subscriberWithContext.valueInOnComplete.get()).isEqualTo("ref_init");
		}

		// Fundamental tests for Mono

		@Test
		void monoSubscribe() {
			assertThreadLocalsPresentInMono(this::threadSwitchingMono, true);
		}

		@Test
		void internalMonoFlatMapSubscribe() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("hello")
					    .flatMap(item -> threadSwitchingMono()));
		}

		@Test
		void internalMonoFlatMapSubscribeNoFusion() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("hello")
					    .hide()
					    .flatMap(item -> threadSwitchingMono()));
		}

		@Test
		void directMonoSubscribeAsCoreSubscriber() throws InterruptedException, TimeoutException {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			CoreSubscriberWithContext<String> subscriberWithContext =
					new CoreSubscriberWithContext<>();

			mono.subscribe(subscriberWithContext);

			if (!subscriberWithContext.latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(subscriberWithContext.error.get()).isNull();
			assertThat(subscriberWithContext.complete.get()).isTrue();

			// We can't do anything here. subscribe(CoreSubscriber) is abstract in
			// CoreSubscriber interface and we have no means to intercept the calls to
			// restore ThreadLocals.
			assertThat(subscriberWithContext.valueInOnNext.get()).isEqualTo("ref_init");
			assertThat(subscriberWithContext.valueInOnComplete.get()).isEqualTo("ref_init");
		}

		// Flux tests

		@Test
		void fluxCreate() {
			Supplier<Flux<?>> fluxSupplier =
					() -> Flux.create(sink -> executorService.submit(() -> {
				sink.next("Hello");
				sink.complete();
			}));

			assertThreadLocalsPresentInFlux(fluxSupplier);
		}

		@Test
		void fluxMap() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().map(String::toUpperCase));
		}

		@Test
		void fluxIgnoreThenSwitchThread() {
			assertThreadLocalsPresentInMono(() -> Flux.just("Bye").then(threadSwitchingMono()));
		}

		@Test
		void fluxSwitchThreadThenIgnore() {
			assertThreadLocalsPresentInMono(() -> threadSwitchingFlux().then(Mono.just("Hi")));
		}

		@Test
		void fluxDeferContextual() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.deferContextual(ctx -> threadSwitchingFlux()));
		}

		@Test
		void fluxFirstWithSignalArray() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithSignal(threadSwitchingFlux()));
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithSignal(threadSwitchingFlux()).or(threadSwitchingFlux()));
		}

		@Test
		void fluxFirstWithSignalIterable() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithSignal(Collections.singletonList(threadSwitchingFlux())));
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithSignal(Stream.of(threadSwitchingFlux(), threadSwitchingFlux()).collect(Collectors.toList())));
		}

		@Test
		void fluxRetryWhen() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux().retryWhen(Retry.max(1)));
		}

		@Test
		void fluxRetryWhenSwitchingThread() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.error(new ExpectedException("Oops"))
					    .retryWhen(Retry.from(f -> threadSwitchingFlux())));
		}

		@Test
		void fluxRepeatWhen() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.repeatWhen(s -> Flux.just(1)));
		}

		@Test
		void fluxRepeatWhenSwitchingThread() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello")
					    .repeatWhen(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxWindowUntil() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux().windowUntil(s -> true)
					                     .flatMap(Function.identity()));
		}

		@Test
		void switchOnFirst() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.switchOnFirst((s, f) -> f.map(String::toUpperCase)));
		}

		@Test
		void switchOnFirstFuseable() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.filter("Hello"::equals)
							.switchOnFirst((s, f) -> f.map(String::toUpperCase)));
		}

		@Test
		void switchOnFirstSwitchThread() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.switchOnFirst((s, f) -> threadSwitchingFlux()));
		}

		@Test
		void switchOnFirstFuseableSwitchThread() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.filter("Hello"::equals)
							.switchOnFirst((s, f) -> threadSwitchingFlux()));
		}

		@Test
		void fluxWindowTimeout() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
							.windowTimeout(1, Duration.ofDays(1), true));
		}

		@Test
		void fluxMergeComparing() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.mergeComparing(Flux.empty(), threadSwitchingFlux()));
		}

		@Test
		void fluxFirstWithValueArray() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithValue(Flux.empty(), threadSwitchingFlux()));
		}

		@Test
		void fluxFirstWithValueIterable() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.firstWithValue(
							Stream.of(Flux.<String>empty(), threadSwitchingFlux())
							      .collect(Collectors.toList())));
		}

		@Test
		void fluxConcatArray() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.concat(Mono.empty(), threadSwitchingFlux()));
		}

		@Test
		void fluxConcatIterable() {
			assertThreadLocalsPresent(
					Flux.concat(
							Stream.of(Flux.<String>empty(), threadSwitchingFlux()).collect(Collectors.toList())));

			// Direct subscription
		}

		@Test
		void fluxGenerate() {
			assertThreadLocalsPresentInFlux(() -> Flux.generate(sink -> {
				sink.next("Hello");
				// the generator is checked if any signal was delivered by the consumer
				// so we perform asynchronous completion only
				executorService.submit(sink::complete);
			}));
		}

		@Test
		void fluxCombineLatest() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.combineLatest(
							Flux.just(""), threadSwitchingFlux(), (s1, s2) -> s2));
		}

		@Test
		void fluxUsing() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.using(() -> 0, i -> threadSwitchingFlux(), i -> {}));
		}

		@Test
		void fluxZip() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.zip(Flux.just(""), threadSwitchingFlux()));
		}

		@Test
		void fluxZipIterable() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.zip(Stream.of(Flux.just(""), threadSwitchingFlux()).collect(Collectors.toList()),
					obj -> Tuples.of((String) obj[0], (String) obj[1])));
		}

		@Test
		void fluxBufferBoundary() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").delayElements(Duration.ofMillis(20))
					    .buffer(threadSwitchingFlux()));
		}

		@Test
		void fluxBufferWhen() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello").delayElements(Duration.ofMillis(20))
					    .bufferWhen(threadSwitchingFlux(), x -> Flux.empty()));
		}

		@Test
		void fluxConcatMap() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .concatMap(s -> threadSwitchingFlux(), 1));
		}

		@Test
		void fluxConcatMapNoPrefetch() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello").hide()
					    .concatMap(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxDelaySubscription() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello")
					    .delaySubscription(threadSwitchingFlux()));
		}

		@Test
		void fluxExpand() {
			AtomicBoolean done = new AtomicBoolean(false);
			// We don't validate direct subscription via CoreSubscriber with Context in
			// this case as it can happen that the drain loop is in the main thread
			// and won't restore TLs from the Context when contextWrite operator is
			// missing along the way in the chain.
			assertThreadLocalsPresent(
					Flux.just("hello").expand(s -> {
						if (done.get()) {
							return Flux.empty();
						} else {
							done.set(true);
							return threadSwitchingFlux();
						}
					}));
		}

		@Test
		void fluxFilterWhen() {
			// We don't validate direct subscription via CoreSubscriber with Context in
			// this case as it can happen that the drain loop is in the main thread
			// and won't restore TLs from the Context when contextWrite operator is
			// missing along the way in the chain.
			assertThreadLocalsPresent(
					Flux.just("hello")
					    .filterWhen(s -> new ThreadSwitchingFlux<>(Boolean.TRUE, executorService)));
		}

		@Test
		void fluxGroupJoinFlattened() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello").groupJoin(threadSwitchingFlux(),
							l -> Flux.never(), r -> Flux.never(),
							(s, f) -> f.map(i -> s)).flatMap(Function.identity()));
		}

		@Test
		void fluxGroupJoin() {
			assertThreadLocalsPresent(
					Flux.just("hello").groupJoin(threadSwitchingFlux(),
							l -> Flux.never(), r -> Flux.never(),
							(s, f) -> f.map(i -> s)));

			// works only with contextWrite because the group is delivered using the
			// signal from the left hand side
		}

		@Test
		void fluxGroupJoinSubscribed() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello").groupJoin(threadSwitchingFlux(),
							l -> Flux.never(), r -> Flux.never(),
							(s, f) -> f.map(i -> s))
					    .flatMap(Function.identity()));
		}

		@Disabled("Only contextWrite/contextCapture usages are supported")
		@Test
		void fluxJustRawSubscribe() {
			assertThatNoException().isThrownBy(() ->
				assertThatThreadLocalsPresentDirectRawSubscribe(Flux.just("hello"))
			);
		}

		@Test
		void fluxJoin() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("hello").join(threadSwitchingFlux(), l -> Flux.never(),
							r -> Flux.never(), (s1, s2) -> s1 + s2));
		}

		@Test
		void fluxLift() {
			assertThreadLocalsPresentInFlux(() -> {
				Flux<String> flux = Flux.just("Hello").hide();

				Publisher<String> lifted =
						Operators.<String, String>liftPublisher((pub, sub) -> new CoreSubscriber<String>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(String s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }

							         @Override
							         public Context currentContext() {
								         return sub.currentContext();
							         }
						         })
						         .apply(flux);

				return (Flux<String>) lifted;
			});
		}

		@Test
		void fluxLiftFuseable() {
			assertThreadLocalsPresentInFlux(() -> {
				Flux<String> flux = Flux.just("Hello");

				Publisher<String> lifted =
						Operators.<String, String>liftPublisher((pub, sub) -> new CoreSubscriber<String>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(String s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }
						         })
						         .apply(flux);

				return (Flux<String>) lifted;
			});
		}

		// see https://github.com/reactor/reactor-core/issues/3762
		@Test
		void fluxLiftOnEveryOperator() {
			Function<? super Publisher<Object>, ? extends Publisher<Object>>
					everyOperatorLift = Operators.lift((a, b) -> b);

			Hooks.onEachOperator("testEveryOperatorLift", everyOperatorLift);

			assertThreadLocalsPresentInFlux(() -> Flux.just("Hello").hide()
			                                          .publish().refCount().map(s -> s));

			Hooks.resetOnEachOperator();
		}

		@Test
		void fluxFlatMapSequential() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .flatMapSequential(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxOnErrorResume() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.error(new ExpectedException("Oops"))
					    .onErrorResume(t -> threadSwitchingFlux()));
		}

		@Test
		void fluxPublishMulticast() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello")
							.publish(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxSkipUntilOther() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .skipUntilOther(threadSwitchingFlux()));
		}

		@Test
		void fluxSample() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").concatWith(Flux.never())
					    .sample(threadSwitchingFlux()));
		}

		@Test
		void fluxSampleFirst() {
			// We don't validate direct subscription via CoreSubscriber with Context in
			// this case as it can happen that the drain loop is in the main thread
			// and won't restore TLs from the Context when contextWrite operator is
			// missing along the way in the chain.
			assertThreadLocalsPresent(
					Flux.just("Hello").concatWith(Flux.never())
					    .sampleFirst(s -> new ThreadSwitchingFlux<>(new ExpectedException("oops"), executorService)));
		}

		@Test
		void fluxSampleTimeout() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux().concatWith(Mono.delay(Duration.ofMillis(10)).map(l -> "").concatWith(Mono.empty()))
					    .sampleTimeout(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxSwitchIfEmpty() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.empty()
					    .switchIfEmpty(threadSwitchingFlux()));
		}

		@Test
		void fluxSwitchMapNoPrefetch() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .switchMap(s -> threadSwitchingFlux()));
		}

		@Test
		void fluxSwitchMap() {
			assertThreadLocalsPresentInFlux(() ->
					threadSwitchingFlux()
					    .switchMap(s -> threadSwitchingFlux(), 1));
		}

		@Test
		void fluxTakeUntilOther() {
			// We don't validate direct subscription via CoreSubscriber with Context in
			// this case as it can happen that the drain loop is in the main thread
			// and won't restore TLs from the Context when contextWrite operator is
			// missing along the way in the chain.
			assertThreadLocalsPresent(
					Flux.concat(Flux.just("Hello"), Flux.never())
					    .takeUntilOther(threadSwitchingFlux()));
		}

		@Test
		void fluxTimeoutFirst() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.never()
					    .timeout(threadSwitchingFlux()));
		}

		@Test
		void fluxTimeoutOther() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.never()
					    .timeout(threadSwitchingFlux(), i -> Flux.never(), threadSwitchingFlux()));
		}

		@Test
		void fluxWindowBoundary() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").delayElements(Duration.ofMillis(20))
							.window(threadSwitchingFlux()));
		}

		@Test
		void fluxWindowBoundaryFlattened() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").delayElements(Duration.ofMillis(20))
					    .window(threadSwitchingFlux())
					    .flatMap(Function.identity()));
		}

		@Test
		@Disabled("Publisher delivering the window has no notion of Context so nothing " +
				"can be restored in onNext")
		void fluxWindowWhen() {
			assertThreadLocalsPresent(
					threadSwitchingFlux()
					    .windowWhen(threadSwitchingFlux(), s -> threadSwitchingFlux()));
		}

		@Test
		@Disabled("Publisher delivering the window has no notion of Context so nothing " +
				"can be restored in onNext")
		void fluxDelayedWindowWhen() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").delayElements(Duration.ofMillis(100))
					    .windowWhen(threadSwitchingFlux(), s -> threadSwitchingFlux()));
		}

		@Test
		@Disabled("Publisher completing the window has no notion of Context so nothing " +
				"can be restored in onComplete")
		void fluxWindowWhenFlatMapped() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.just("Hello").delayElements(Duration.ofMillis(100))
					    .windowWhen(threadSwitchingFlux(), s -> threadSwitchingFlux())
					    .flatMap(Function.identity()));
		}

		@Test
		void fluxWithLatestFrom() {
			// We don't validate direct subscription via CoreSubscriber with Context in
			// this case as it can happen that the drain loop is in the main thread
			// and won't restore TLs from the Context when contextWrite operator is
			// missing along the way in the chain.
			assertThreadLocalsPresent(
					Flux.just("Hello")
					    .withLatestFrom(threadSwitchingFlux(), (s1, s2) -> s1));
		}

		@Test
		void continuationBrokenByThreadSwitch() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.concat(Mono.empty(), threadSwitchingMono().retry()));
		}

		// Mono tests

		@Test
		void monoCreate() {
			assertThreadLocalsPresentInMono(() ->
					Mono.create(sink -> {
						executorService.submit(() -> {
							sink.success("Hello");
						});
					}));
		}

		@Test
		void monoSwitchThreadIgnoreThen() {
			assertThreadLocalsPresentInMono(() ->
					threadSwitchingMono().then(Mono.just("Bye")));
		}

		@Test
		void monoIgnoreThenSwitchThread() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("Bye").then(threadSwitchingMono()));
		}

		@Test
		void monoSwitchThreadDelayUntil() {
			assertThreadLocalsPresentInMono(() ->
					threadSwitchingMono().delayUntil(s -> Mono.delay(Duration.ofMillis(1))));
		}

		@Test
		void monoDelayUntilSwitchingThread() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("Hello").delayUntil(s -> threadSwitchingMono()));
		}

		@Test
		void monoIgnoreSwitchingThread() {
			assertThreadLocalsPresentInMono(() ->
					Mono.ignoreElements(threadSwitchingMono()));
		}

		@Test
		void monoDeferContextual() {
			assertThreadLocalsPresentInMono(() ->
					Mono.deferContextual(ctx -> threadSwitchingMono()));
		}

		@Test
		void monoDefer() {
			assertThreadLocalsPresentInMono(() ->
					Mono.defer(this::threadSwitchingMono));
		}

		@Test
		void monoFirstWithSignalArray() {
			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithSignal(threadSwitchingMono()));

			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithSignal(threadSwitchingMono())
					    .or(threadSwitchingMono()));
		}

		@Test
		void monoFirstWithSignalIterable() {
			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithSignal(Collections.singletonList(threadSwitchingMono())));

			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithSignal(
							Stream.of(threadSwitchingMono(), threadSwitchingMono())
							      .collect(Collectors.toList())));
		}

		@Test
		void monoFromFluxSingle() {
			assertThreadLocalsPresentInMono(() ->
					threadSwitchingFlux().single());
		}

		@Test
		void monoRetryWhen() {
			assertThreadLocalsPresentInMono(() ->
					threadSwitchingMono().retryWhen(Retry.max(1)));
		}

		@Test
		void monoRetryWhenSwitchingThread() {
			assertThreadLocalsPresentInMono(() ->
					Mono.error(new ExpectedException("Oops"))
					    .retryWhen(Retry.from(f -> threadSwitchingMono())));
		}

		@Test
		void monoUsing() {
			assertThreadLocalsPresentInMono(() ->
					Mono.using(() -> "Hello",
							seed -> threadSwitchingMono(),
							seed -> {},
							false));
		}

		@Test
		void monoFirstWithValueArray() {
			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithValue(Mono.empty(), threadSwitchingMono()));
		}

		@Test
		void monoFirstWithValueIterable() {
			assertThreadLocalsPresentInMono(() ->
					Mono.firstWithValue(
							Stream.of(Mono.<String>empty(), threadSwitchingMono())
							      .collect(Collectors.toList())));
		}

		@Test
		void monoZip() {
			assertThreadLocalsPresentInMono(() ->
					Mono.zip(Mono.just(""), threadSwitchingMono()));
		}

		@Test
		void monoZipIterable() {
			assertThreadLocalsPresentInMono(() ->
					Mono.zip(
							Stream.of(Mono.just(""), threadSwitchingMono())
							      .collect(Collectors.toList()),
							obj -> Tuples.of((String) obj[0], (String) obj[1])));
		}

		@Test
		void monoSequenceEqual() {
			assertThreadLocalsPresentInMono(() ->
					Mono.sequenceEqual(Mono.just("Hello"), threadSwitchingMono()));
		}

		@Test
		void monoWhen() {
			assertThreadLocalsPresentInMono(() ->
					Mono.when(Mono.empty(), threadSwitchingMono()));
		}

		@Test
		void monoUsingWhen() {
			assertThreadLocalsPresentInMono(() ->
					Mono.usingWhen(Mono.just("Hello"), s -> threadSwitchingMono(),
							s -> Mono.empty()));
		}

		@Test
		void monoFlatMapMany() {
			assertThreadLocalsPresentInFlux(() ->
					Mono.just("hello")
						.hide()
					    .flatMapMany(item -> threadSwitchingFlux()));
		}

		@Test
		void monoFlatMapManyFuseable() {
			assertThreadLocalsPresentInFlux(() ->
					Mono.just("hello")
					    .flatMapMany(item -> threadSwitchingFlux()));
		}

		@Test
		void monoDelaySubscription() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("Hello").delaySubscription(threadSwitchingMono()));
		}

		@Test
		void monoFilterWhen() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("Hello").hide()
					    .filterWhen(s -> new ThreadSwitchingMono<>(Boolean.TRUE, executorService)));
		}

		@Test
		void monoLift() {
			assertThreadLocalsPresentInMono(() -> {
				Mono<String> mono = Mono.just("Hello").hide();

				Publisher<String> lifted =
						Operators.<String, String>liftPublisher((pub, sub) -> new CoreSubscriber<String>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(String s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }
						         })
						         .apply(mono);

				return (Mono<String>) lifted;
			});
		}

		@Test
		void monoLiftFuseable() {
			assertThreadLocalsPresentInMono(() -> {
				Mono<String> mono = Mono.just("Hello");

				Publisher<String> lifted =
						Operators.<String, String>liftPublisher((pub, sub) -> new CoreSubscriber<String>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(String s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }
						         })
						         .apply(mono);

				return (Mono<String>) lifted;
			});
		}

		@Test
		void monoOnErrorResume() {
			assertThreadLocalsPresentInMono(() ->
					Mono.error(new ExpectedException("oops"))
							.onErrorResume(e -> threadSwitchingMono()));
		}

		@Test
		void monoPublishMulticast() {
			assertThreadLocalsPresentInMono(() ->
					Mono.just("Hello")
					    .publish(s -> threadSwitchingMono()));
		}

		@Test
		void monoSwitchIfEmpty() {
			assertThreadLocalsPresentInMono(() ->
					Mono.empty()
					    .switchIfEmpty(threadSwitchingMono()));
		}

		@Test
		void monoTakeUntilOther() {
			assertThreadLocalsPresentInMono(() ->
					Mono.delay(Duration.ofDays(1)).then(Mono.just("Hello"))
					    .takeUntilOther(threadSwitchingMono()));
		}

		@Test
		void monoTimeoutFirst() {
			assertThreadLocalsPresentInMono(() ->
					Mono.never().timeout(threadSwitchingMono()));
		}

		@Test
		void monoTimeoutFallback() {
			assertThreadLocalsPresentInMono(() ->
					Mono.never().timeout(threadSwitchingMono(), threadSwitchingMono()));
		}

		// ParallelFlux tests

		@Test
		void parallelFluxFromMonoToMono() {
			assertThreadLocalsPresentInMono(() ->
					Mono.from(ParallelFlux.from(threadSwitchingMono())));
		}

		@Test
		void parallelFluxFromMonoToFlux() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.from(ParallelFlux.from(threadSwitchingMono())));
		}

		@Test
		void parallelFluxFromFluxToMono() {
			assertThreadLocalsPresentInMono(() ->
					Mono.from(ParallelFlux.from(threadSwitchingFlux())));
		}

		@Test
		void parallelFluxFromFluxToFlux() {
			assertThreadLocalsPresentInFlux(() ->
					Flux.from(ParallelFlux.from(threadSwitchingFlux())));
		}

		@Test
		void parallelFluxLift() {
			assertThreadLocalsPresentInFlux(() -> {
				ParallelFlux<String> parallelFlux = ParallelFlux.from(Flux.just("Hello"));

				Publisher<String> lifted =
						Operators.<String, String>liftPublisher((pub, sub) -> new CoreSubscriber<String>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(String s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }
						         })
						         .apply(parallelFlux);

						return ((ParallelFlux<?>) lifted).sequential();
					});
		}

		@Test
		void parallelFluxLiftFuseable() {
			assertThreadLocalsPresentInFlux(() -> {
				ParallelFlux<ArrayList<String>> parallelFlux =
						ParallelFlux.from(Flux.just("Hello"))
						            .collect(ArrayList<String>::new, ArrayList::add);

				Publisher<ArrayList<String>> lifted =
						Operators.<ArrayList<String>, ArrayList<String>>liftPublisher((pub, sub) -> new CoreSubscriber<ArrayList<String>>() {
							         @Override
							         public void onSubscribe(Subscription s) {
								         executorService.submit(() -> sub.onSubscribe(s));
							         }

							         @Override
							         public void onNext(ArrayList<String> s) {
								         executorService.submit(() -> sub.onNext(s));
							         }

							         @Override
							         public void onError(Throwable t) {
								         executorService.submit(() -> sub.onError(t));
							         }

							         @Override
							         public void onComplete() {
								         executorService.submit(sub::onComplete);
							         }
						         })
						         .apply(parallelFlux);

				return ((ParallelFlux<?>) lifted).sequential();
			});
		}

		@Test
		void parallelFluxFromThreadSwitchingMono() {
			assertThreadLocalsPresentInFlux(() ->
					ParallelFlux.from(threadSwitchingMono()).sequential());
		}

		@Test
		void parallelFluxFromThreadSwitchingFlux() {
			assertThreadLocalsPresentInFlux(() ->
					ParallelFlux.from(threadSwitchingFlux()).sequential());
		}

		@Test
		void threadSwitchingParallelFluxSequential() {
			AtomicReference<String> value = new AtomicReference<>();
			new ThreadSwitchingParallelFlux<>("Hello", executorService)
					.sequential()
					.doOnNext(i -> value.set(REF.get()))
					.contextWrite(Context.of(KEY, "present"))
					.blockLast();

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void threadSwitchingParallelFluxThen() {
			assertThreadLocalsPresentInMono(() ->
					new ThreadSwitchingParallelFlux<>("Hello", executorService)
							.then());
		}

		@Test
		void threadSwitchingParallelFluxOrdered() {
			assertThreadLocalsPresentInFlux(() ->
					new ThreadSwitchingParallelFlux<>("Hello", executorService)
							.ordered(Comparator.naturalOrder()));
		}

		@Test
		void threadSwitchingParallelFluxReduce() {
			AtomicReference<String> value = new AtomicReference<>();
			new ThreadSwitchingParallelFlux<>("Hello", executorService)
					.reduce((s1, s2) -> s2)
					.doOnNext(i -> value.set(REF.get()))
					.contextWrite(Context.of(KEY, "present"))
					.block();

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void threadSwitchingParallelFluxReduceSeed() {
			AtomicReference<String> value = new AtomicReference<>();
			new ThreadSwitchingParallelFlux<>("Hello", executorService)
					.reduce(ArrayList::new, (l, s) -> {
						value.set(REF.get());
						l.add(s);
						return l;
					})
					.sequential()
					.contextWrite(Context.of(KEY, "present"))
					.blockLast();

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void threadSwitchingParallelFluxGroup() {
			AtomicReference<String> value = new AtomicReference<>();
			new ThreadSwitchingParallelFlux<>("Hello", executorService)
					.groups()
					.doOnNext(i -> value.set(REF.get()))
					.flatMap(Flux::last)
					.contextWrite(Context.of(KEY, "present"))
					.blockLast();

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void threadSwitchingParallelFluxSort() {
			assertThreadLocalsPresentInFlux(() ->
					new ThreadSwitchingParallelFlux<>("Hello", executorService)
							.sorted(Comparator.naturalOrder()));
		}

		// ConnectableFlux tests

		@Test
		void threadSwitchingAutoConnect() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingConnectableFlux().autoConnect());
		}

		@Test
		void threadSwitchingRefCount() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingConnectableFlux().refCount());
		}

		@Test
		void threadSwitchingRefCountGrace() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingConnectableFlux().refCount(1, Duration.ofMillis(100)));
		}

		@Test
		void threadSwitchingPublishAutoConnect() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().publish().autoConnect());
		}

		@Test
		void threadSwitchingPublishRefCount() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().publish().refCount());
		}

		@Test
		void threadSwitchingPublishRefCountGrace() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().publish().refCount(1, Duration.ofMillis(100)));
		}

		@Test
		void threadSwitchingMonoPublish() {
			assertThreadLocalsPresentInMono(() -> threadSwitchingMono().publish(Function.identity()));
		}

		@Test
		void threadSwitchingMonoPublishSwitchingThread() {
			assertThreadLocalsPresentInMono(() -> threadSwitchingMono().publish(m -> threadSwitchingMono()));
		}

		@Test
		void threadSwitchingReplayAutoConnect() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().replay(1).autoConnect());
		}

		@Test
		void threadSwitchingReplayRefCount() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().replay(1).refCount());
		}

		@Test
		void threadSwitchingReplayRefCountGrace() {
			assertThreadLocalsPresentInFlux(() -> threadSwitchingFlux().replay(1).refCount(1,	Duration.ofMillis(100)));
		}

		// Sinks tests

		@Test
		void sink() throws InterruptedException, TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.One<Integer> sink = Sinks.one();

			sink.asMono()
			            .doOnNext(i -> {
							value.set(REF.get());
							latch.countDown();
			            })
			            .contextWrite(Context.of(KEY, "present"))
			            .subscribe();

			executorService.submit(() -> sink.tryEmitValue(1));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinkDirect() throws InterruptedException, TimeoutException, ExecutionException {
			Sinks.One<String> sink1 = Sinks.one();
			assertThatThreadLocalsPresentDirectCoreSubscribe(sink1.asMono(),
					() -> sink1.tryEmitValue("Hello"));

			Sinks.One<String> sink2 = Sinks.one();
			assertThatThreadLocalsPresentDirectRawSubscribe(sink2.asMono(),
					() -> sink2.tryEmitValue("Hello"));
		}

		@Test
		void sinksEmpty() throws InterruptedException, TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.Empty<Void> spec = Sinks.empty();

			spec.asMono()
			    .doOnSuccess(ignored -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService.submit(spec::tryEmitEmpty);

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinksEmptyDirect() throws InterruptedException, TimeoutException {
			Sinks.Empty<Object> empty1 = Sinks.empty();
			assertThatThreadLocalsPresentDirectCoreSubscribe(empty1.asMono(), empty1::tryEmitEmpty);

			Sinks.Empty<Object> empty2 = Sinks.empty();
			assertThatThreadLocalsPresentDirectRawSubscribe(empty2.asMono(), empty2::tryEmitEmpty);
		}

		@Test
		void sinkManyUnicast() throws InterruptedException, TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.ManySpec spec = Sinks.many();

			Sinks.Many<String> many = spec.unicast()
			                               .onBackpressureBuffer();
			many.asFlux()
			       .doOnNext(i -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			       .contextWrite(Context.of(KEY, "present"))
			       .subscribe();

			executorService.submit(() -> many.tryEmitNext("Hello"));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinkManyUnicastDirect() throws InterruptedException, TimeoutException {
			Sinks.Many<String> many1 = Sinks.many().unicast()
			                              .onBackpressureBuffer();

			assertThatThreadLocalsPresentDirectCoreSubscribe(many1.asFlux(), () -> {
				many1.tryEmitNext("Hello");
				many1.tryEmitComplete();
			});

			Sinks.Many<String> many2 = Sinks.many().unicast()
			                                .onBackpressureBuffer();

			assertThatThreadLocalsPresentDirectRawSubscribe(many2.asFlux(), () -> {
				many2.tryEmitNext("Hello");
				many2.tryEmitComplete();
			});
		}

		@Test
		void sinkManyUnicastNoBackpressure() throws InterruptedException,
		                                           TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.ManySpec spec = Sinks.many();

			Sinks.Many<String> many = spec.unicast().onBackpressureError();
			many.asFlux()
			    .doOnNext(i -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService.submit(() -> many.tryEmitNext("Hello"));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinkManyMulticastAllOrNothing() throws InterruptedException,
		                                           TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.ManySpec spec = Sinks.many();

			Sinks.Many<String> many = spec.multicast().directAllOrNothing();
			many.asFlux()
			    .doOnNext(i -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService.submit(() -> many.tryEmitNext("Hello"));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinkManyMulticastBuffer() throws InterruptedException, TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.ManySpec spec = Sinks.many();

			Sinks.Many<String> many = spec.multicast().onBackpressureBuffer();
			many.asFlux()
			    .doOnNext(i -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService.submit(() -> many.tryEmitNext("Hello"));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void sinkManyMulticastBestEffort() throws InterruptedException, TimeoutException {
			AtomicReference<String> value = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);

			Sinks.ManySpec spec = Sinks.many();

			Sinks.Many<String> many = spec.multicast().directBestEffort();
			many.asFlux()
			    .doOnNext(i -> {
				    value.set(REF.get());
				    latch.countDown();
			    })
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService.submit(() -> many.tryEmitNext("Hello"));

			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("timed out");
			}

			assertThat(value.get()).isEqualTo("present");
		}

		// Other

		List<Class<?>> getAllClassesInClasspathRecursively(File directory) throws Exception {
			List<Class<?>> classes = new ArrayList<>();

			for (File file : directory.listFiles()) {
				if (file.isDirectory()) {
					classes.addAll(getAllClassesInClasspathRecursively(file));
				} else if (file.getName().endsWith(".class") ) {
					String path = file.getPath();
					path = path.replace("./build/classes/java/main/reactor/", "");
					String pkg = path.substring(0, path.lastIndexOf("/") + 1).replace("/",
							".");
					String name = path.substring(path.lastIndexOf("/") + 1).replace(".class", "");
					try {
						classes.add(Class.forName("reactor." + pkg + name));
					}
					catch (ClassNotFoundException ex) {
						System.out.println("Ignoring " + pkg + name);
					} catch (NoClassDefFoundError err) {
						System.out.println("Ignoring " + pkg + name);
					}
				}
			}

			return classes;
		}

		@Test
		@Disabled("Used to find Publishers that can switch threads")
		void printInterestingClasses() throws Exception {
			List<Class<?>> allClasses =
					getAllClassesInClasspathRecursively(new File("./build/classes/java/main/reactor/"));

			System.out.println("Classes that are Publisher, but not SourceProducer, " +
					"ConnectableFlux, ParallelFlux, GroupedFlux, MonoFromFluxOperator, " +
					"FluxFromMonoOperator:");
			for (Class<?> c : allClasses) {
				if (Publisher.class.isAssignableFrom(c) && !SourceProducer.class.isAssignableFrom(c)
						&& !ConnectableFlux.class.isAssignableFrom(c)
						&& !ParallelFlux.class.isAssignableFrom(c)
						&& !GroupedFlux.class.isAssignableFrom(c)
						&& !MonoFromFluxOperator.class.isAssignableFrom(c)
						&& !FluxFromMonoOperator.class.isAssignableFrom(c)) {
					if (Flux.class.isAssignableFrom(c) && !FluxOperator.class.isAssignableFrom(c)) {
						System.out.println(c.getName());
					}
					if (Mono.class.isAssignableFrom(c) && !MonoOperator.class.isAssignableFrom(c)) {
						System.out.println(c.getName());
					}
				}
			}

			System.out.println("Classes that are Fuseable and Publisher but not Mono or Flux, ?");
			for (Class<?> c : allClasses) {
				if (Fuseable.class.isAssignableFrom(c) && Publisher.class.isAssignableFrom(c)
						&& !Mono.class.isAssignableFrom(c)
						&& !Flux.class.isAssignableFrom(c)) {
					System.out.println(c.getName());
				}
			}
		}

		private class ExpectedException extends RuntimeException {

			public ExpectedException(String message) {
				super(message);
			}
			
		}
		private class CoreSubscriberWithContext<T> implements CoreSubscriber<T> {

			final AtomicReference<String>    valueInOnNext;
			final AtomicReference<String>    valueInOnComplete;
			final AtomicReference<String>    valueInOnError;
			final AtomicReference<Throwable> error;
			final CountDownLatch             latch;
			final AtomicBoolean              complete;
			final AtomicBoolean              hadNext;

			public CoreSubscriberWithContext() {
				this.valueInOnNext = new AtomicReference<>();
				this.valueInOnComplete = new AtomicReference<>();
				this.valueInOnError = new AtomicReference<>();
				this.error = new AtomicReference<>();
				this.complete = new AtomicBoolean();
				this.hadNext = new AtomicBoolean();
				this.latch = new CountDownLatch(1);
			}

			@Override
			public Context currentContext() {
				return Context.of(KEY, "present");
			}

			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(T t) {
				hadNext.set(true);
				valueInOnNext.set(REF.get());
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				valueInOnError.set(REF.get());
				latch.countDown();
			}

			@Override
			public void onComplete() {
				complete.set(true);
				valueInOnComplete.set(REF.get());
				latch.countDown();
			}
		}
	}

	@Nested
	class NonReactorSources {
		@Test
		void fluxFromPublisher() throws InterruptedException, ExecutionException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Flux.from(nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void fluxFlatMapToPublisher() throws InterruptedException, ExecutionException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Flux.just("hello")
				.flatMap(s -> nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void monoFlatMapToPublisher() throws InterruptedException, ExecutionException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Mono.just("hello")
			    .hide()
			    .flatMapMany(s -> nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void monoFromPublisher() throws InterruptedException, ExecutionException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Mono.from(nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void monoFromPublisherIgnoringContract()
				throws InterruptedException, ExecutionException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Mono.fromDirect(nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void monoFromCompletionStage() throws ExecutionException, InterruptedException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();

			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<String> value = new AtomicReference<>();

			// we need to delay delivery to ensure the completion signal is delivered
			// on a Thread from executorService
			CompletionStage<String> completionStage = CompletableFuture.supplyAsync(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					// ignore
				}
				return "test";
			}, executorService);

			TestSubscriber<String> testSubscriber = TestSubscriber.create();

			Mono.fromCompletionStage(completionStage)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe(testSubscriber);

			latch.countDown();
			testSubscriber.block();

			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void monoFromFuture() throws ExecutionException, InterruptedException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();

			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<String> value = new AtomicReference<>();

			// we need to delay delivery to ensure the completion signal is delivered
			// on a Thread from executorService
			CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					// ignore
				}
				return "test";
			}, executorService);

			TestSubscriber<String> testSubscriber = TestSubscriber.create();

			Mono.fromFuture(future)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe(testSubscriber);

			latch.countDown();
			testSubscriber.block();

			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void fluxMerge() throws ExecutionException, InterruptedException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();
			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			Flux.merge(Flux.empty(), nonReactorPublisher)
			    .doOnNext(s -> value.set(REF.get()))
			    .contextWrite(Context.of(KEY, "present"))
			    .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();
			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}

		@Test
		void parallelFlux() throws ExecutionException, InterruptedException {
			ExecutorService executorService = Executors.newSingleThreadExecutor();

			AtomicReference<String> value = new AtomicReference<>();

			TestPublisher<String> testPublisher = TestPublisher.create();
			Publisher<String> nonReactorPublisher = testPublisher;

			ParallelFlux.from(nonReactorPublisher)
			            .doOnNext(i -> value.set(REF.get()))
			            .sequential()
			            .contextWrite(Context.of(KEY, "present"))
			            .subscribe();

			executorService
					.submit(() -> testPublisher.emit("test").complete())
					.get();

			testPublisher.assertWasSubscribed();
			testPublisher.assertWasNotCancelled();
			testPublisher.assertWasRequested();

			assertThat(value.get()).isEqualTo("present");

			// validate there are no leftovers for other tasks to be attributed to
			// previous values
			executorService.submit(() -> value.set(REF.get())).get();

			assertThat(value.get()).isEqualTo("ref_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref_init");

			executorService.shutdownNow();
		}
	}

	@Nested
	class BlockingOperatorsAutoCapture {

		@Test
		void monoBlock() {
			AtomicReference<String> value = new AtomicReference<>();

			REF.set("present");

			Mono.just("test")
			    // Introduce an artificial barrier to clear ThreadLocals if no Context
			    // is defined in the downstream chain. If block does the job well,
			    // it should have captured the existing ThreadLocal into the Context.
			    .contextWrite(Context.empty())
			    .doOnNext(ignored -> value.set(REF.get()))
			    .block();

			// First, assert the existing ThreadLocal was not cleared.
			assertThat(REF.get()).isEqualTo("present");

			// Now let's find out that it was automatically transferred.
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void monoBlockOptional() {
			AtomicReference<String> value = new AtomicReference<>();

			REF.set("present");

			Mono.empty()
			    // Introduce an artificial barrier to clear ThreadLocals if no Context
			    // is defined in the downstream chain. If block does the job well,
			    // it should have captured the existing ThreadLocal into the Context.
			    .contextWrite(Context.empty())
			    .doOnTerminate(() -> value.set(REF.get()))
			    .blockOptional();

			// First, assert the existing ThreadLocal was not cleared.
			assertThat(REF.get()).isEqualTo("present");

			// Now let's find out that it was automatically transferred.
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void fluxBlockFirst() {
			AtomicReference<String> value = new AtomicReference<>();

			REF.set("present");

			Flux.range(0, 10)
			    // Introduce an artificial barrier to clear ThreadLocals if no Context
			    // is defined in the downstream chain. If block does the job well,
			    // it should have captured the existing ThreadLocal into the Context.
			    .contextWrite(Context.empty())
			    .doOnNext(ignored -> value.set(REF.get()))
			    .blockFirst();

			// First, assert the existing ThreadLocal was not cleared.
			assertThat(REF.get()).isEqualTo("present");

			// Now let's find out that it was automatically transferred.
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void fluxBlockLast() {
			AtomicReference<String> value = new AtomicReference<>();

			REF.set("present");

			Flux.range(0, 10)
			    // Introduce an artificial barrier to clear ThreadLocals if no Context
			    // is defined in the downstream chain. If block does the job well,
			    // it should have captured the existing ThreadLocal into the Context.
			    .contextWrite(Context.empty())
			    .doOnTerminate(() -> value.set(REF.get()))
			    .blockLast();

			// First, assert the existing ThreadLocal was not cleared.
			assertThat(REF.get()).isEqualTo("present");

			// Now let's find out that it was automatically transferred.
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void fluxToIterable() {
			AtomicReference<String> value = new AtomicReference<>();

			REF.set("present");

			Iterable<Integer> integers = Flux.range(0, 10)
			                                 // Introduce an artificial barrier to clear ThreadLocals if no Context
			                                 // is defined in the downstream chain. If block does the job well,
			                                 // it should have captured the existing ThreadLocal into the Context.
			                                 .contextWrite(Context.empty())
			                                 .doOnTerminate(() -> value.set(REF.get()))
			                                 .toIterable();

			assertThat(integers).hasSize(10);

			// First, assert the existing ThreadLocal was not cleared.
			assertThat(REF.get()).isEqualTo("present");

			// Now let's find out that it was automatically transferred.
			assertThat(value.get()).isEqualTo("present");
		}
	}

	@Nested
	class SpecialContextAlteringOperators {

		// The cases here consider operators like doOnDiscard(), which underneath
		// utilize contextWrite() for its purpose. They are special in that we use them
		// internally and do not anticipate the registered keys to be corresponding to
		// any ThreadLocal values. That expectation is reasonable in user facing code
		// as we don't know what keys are used and whether a ThreadLocalAccessor is
		// registered for these keys. Therefore, in specific cases that are internal to
		// reactor-core, we can skip ThreadLocal restoration in fragments of the chain.

		// Explanation of GET/SET operations on TL in the scenarios here:
		// When going UP, we use the value "present".
		// When going DOWN, we clear the value or restore a captured empty value.
		//
		//   1 x GET in block() with implicit context capture
		//
		//   1 x GET going UP from contextWrite (read current to restore later)
		// + 2 x SET going UP from contextWrite + SET restoring current later
		//
		//   1 x GET going DOWN from contextWrite with subscription (read current)
		// + 2 x SET going DOWN from contextWrite + SET restoring current later
		//
		//   1 x GET going UP to request (read current)
		// + 2 x SET going UP from contextWrite + SET restoring current later
		//
		//   1 x GET going DOWN to deliver onComplete (read current)
		// + 2 x SET going DOWN from contextWrite + SET restoring current later

		@Test
		void discardFlux() {
			CountingThreadLocalAccessor accessor = new CountingThreadLocalAccessor();
			ContextRegistry.getInstance().registerThreadLocalAccessor(accessor);

			AtomicInteger tlPresent = new AtomicInteger();
			AtomicInteger discards = new AtomicInteger();

			Flux.just("a")
			    .doOnEach(signal -> {
				    if (CountingThreadLocalAccessor.TL.get().equals("present")) {
					    tlPresent.incrementAndGet();
				    }
			    })
			    .filter(s -> false)
			    .doOnDiscard(String.class, s -> discards.incrementAndGet())
			    .count()
			    .contextWrite(ctx -> ctx.put(CountingThreadLocalAccessor.KEY, "present"))
			    .block();

			assertThat(tlPresent.get()).isEqualTo(2); // 1 x onNext + 1 x onComplete
			assertThat(discards.get()).isEqualTo(1);
			// 5 with doOnDiscard skipping TL restoration, 9 with restoring
			assertThat(accessor.reads.get()).isEqualTo(5);
			// 8 with doOnDiscard skipping TL restoration, 16 with restoring
			assertThat(accessor.writes.get()).isEqualTo(8);

			ContextRegistry.getInstance().removeThreadLocalAccessor(CountingThreadLocalAccessor.KEY);
		}

		@Test
		void discardMono() {
			CountingThreadLocalAccessor accessor = new CountingThreadLocalAccessor();
			ContextRegistry.getInstance().registerThreadLocalAccessor(accessor);

			AtomicInteger tlPresent = new AtomicInteger();
			AtomicInteger discards = new AtomicInteger();

			Mono.just("a")
			    .doOnEach(signal -> {
				    if (CountingThreadLocalAccessor.TL.get().equals("present")) {
					    tlPresent.incrementAndGet();
				    }
			    })
			    .filter(s -> false)
			    .doOnDiscard(String.class, s -> discards.incrementAndGet())
			    .contextWrite(ctx -> ctx.put(CountingThreadLocalAccessor.KEY, "present"))
			    .block();

			assertThat(tlPresent.get()).isEqualTo(2); // 1 x onNext + 1 x onComplete
			assertThat(discards.get()).isEqualTo(1);
			// 5 with doOnDiscard skipping TL restoration, 9 with restoring
			assertThat(accessor.reads.get()).isEqualTo(5);
			// 8 with doOnDiscard skipping TL restoration, 16 with restoring
			assertThat(accessor.writes.get()).isEqualTo(8);

			ContextRegistry.getInstance().removeThreadLocalAccessor(CountingThreadLocalAccessor.KEY);
		}
	}

	private static class CountingThreadLocalAccessor implements ThreadLocalAccessor<String> {
		static final String KEY = "CTLA";
		static final ThreadLocal<String> TL = new ThreadLocal<>();

		AtomicInteger reads = new AtomicInteger();
		AtomicInteger writes = new AtomicInteger();

		@Override
		public Object key() {
			return KEY;
		}

		@Override
		public String getValue() {
			reads.incrementAndGet();
			return TL.get();
		}

		@Override
		public void setValue(String s) {
			writes.incrementAndGet();
			TL.set(s);
		}

		@Override
		public void setValue() {
			writes.incrementAndGet();
			TL.remove();
		}
	}
}

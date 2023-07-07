/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.context.ContextRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.TestSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

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
			    System.out.println(REF.get());
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

		assertThat(REF.get()).isEqualTo("ref1_init");

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
		assertThat(outerThreadLocals).containsOnly("ref1_init").hasSize(size);
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

			assertThat(value.get()).isEqualTo("ref1_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref1_init");

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

			assertThat(value.get()).isEqualTo("ref1_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref1_init");

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

			assertThat(value.get()).isEqualTo("ref1_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref1_init");

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

			assertThat(value.get()).isEqualTo("ref1_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref1_init");

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

			assertThat(value.get()).isEqualTo("ref1_init");

			// validate the current Thread does not have the value set either
			assertThat(REF.get()).isEqualTo("ref1_init");

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
}

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

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.micrometer.context.ContextRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
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
	void threadLocalsPresentInDoAfterTerminate() throws InterruptedException {
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
		latch.await(10, TimeUnit.MILLISECONDS);
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

		void assertThreadLocalPresentInOnNext(Mono<?> chain) {
			AtomicReference<String> value = new AtomicReference<>();

			chain.doOnNext(item -> value.set(REF.get()))
			     .contextWrite(Context.of(KEY, "present"))
			     .block();

			assertThat(value.get()).isEqualTo("present");
		}

		void assertThreadLocalPresentInOnNext(Flux<?> chain) {
			AtomicReference<String> value = new AtomicReference<>();

			chain.doOnNext(item -> value.set(REF.get()))
			     .contextWrite(Context.of(KEY, "present"))
			     .blockLast();

			assertThat(value.get()).isEqualTo("present");
		}

		// Basic tests for Flux

		@Test
		void chainedFluxSubscribe() {
			ThreadSwitchingFlux<String> chain = new ThreadSwitchingFlux<>("Hello", executorService);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void internalFluxSubscribe() {
			ThreadSwitchingFlux<String> inner = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> chain = Flux.just("hello").flatMap(item -> inner);

			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void internalFluxSubscribeNoFusion() {
			ThreadSwitchingFlux<String> inner = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> chain = Flux.just("hello").hide().flatMap(item -> inner);

			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void testFluxSubscriberAsRawSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();

			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);

			TestSubscriber<String> testSubscriber =
					TestSubscriber.builder().contextPut(KEY, "present").build();

			flux
					.doOnNext(i -> value.set(REF.get()))
					.subscribe((Subscriber<? super String>) testSubscriber);

			testSubscriber.block(Duration.ofMillis(10));
			assertThat(testSubscriber.expectTerminalSignal().isOnComplete()).isTrue();
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void testFluxSubscriberAsCoreSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();

			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);

			TestSubscriber<String> testSubscriber =
					TestSubscriber.builder().contextPut(KEY, "present").build();

			flux
					.doOnNext(i -> value.set(REF.get()))
					.subscribe(testSubscriber);

			testSubscriber.block(Duration.ofMillis(10));
			assertThat(testSubscriber.expectTerminalSignal().isOnComplete()).isTrue();
			// Because of onNext in the chain, the internal operator implementation is
			// able to wrap the subscriber and restore the ThreadLocal values
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void directFluxSubscribeAsCoreSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();
			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicBoolean complete = new AtomicBoolean();
			CountDownLatch latch = new CountDownLatch(1);

			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);

			CoreSubscriberWithContext subscriberWithContext =
					new CoreSubscriberWithContext(value, error, latch, complete);

			flux.subscribe(subscriberWithContext);

			latch.await(10, TimeUnit.MILLISECONDS);

			assertThat(error.get()).isNull();
			assertThat(complete.get()).isTrue();

			// We can't do anything here. subscribe(CoreSubscriber) is abstract in
			// CoreSubscriber interface and we have no means to intercept the calls to
			// restore ThreadLocals.
			assertThat(value.get()).isEqualTo("ref_init");
		}

		@Test
		void directFluxSubscribeAsRawSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();
			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicBoolean complete = new AtomicBoolean();
			CountDownLatch latch = new CountDownLatch(1);

			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);

			CoreSubscriberWithContext subscriberWithContext =
					new CoreSubscriberWithContext(value, error, latch, complete);

			// We force the use of subscribe(Subscriber) override instead of
			// subscribe(CoreSubscriber), and we can observe that for such a case we
			// are able to wrap the Subscriber and restore ThreadLocal values for the
			// signals received downstream.
			flux.subscribe((Subscriber<? super String>) subscriberWithContext);

			latch.await(10, TimeUnit.MILLISECONDS);

			assertThat(error.get()).isNull();
			assertThat(complete.get()).isTrue();
			assertThat(value.get()).isEqualTo("present");
		}

		// Basic tests for Mono

		@Test
		void chainedMonoSubscribe() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			assertThreadLocalPresentInOnNext(mono);
		}

		@Test
		void internalMonoSubscribe() {
			Mono<String> inner = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.just("hello").flatMap(item -> inner);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void testMonoSubscriberAsRawSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();

			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			TestSubscriber<String> testSubscriber =
					TestSubscriber.builder().contextPut(KEY, "present").build();

			mono
					.doOnNext(i -> value.set(REF.get()))
					.subscribe((Subscriber<? super String>) testSubscriber);

			testSubscriber.block(Duration.ofMillis(10));
			assertThat(testSubscriber.expectTerminalSignal().isOnComplete()).isTrue();
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void testMonoSubscriberAsCoreSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();

			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			TestSubscriber<String> testSubscriber =
					TestSubscriber.builder().contextPut(KEY, "present").build();

			mono
					.doOnNext(i -> value.set(REF.get()))
					.subscribe(testSubscriber);

			testSubscriber.block(Duration.ofMillis(10));
			assertThat(testSubscriber.expectTerminalSignal().isOnComplete()).isTrue();
			// Because of onNext in the chain, the internal operator implementation is
			// able to wrap the subscriber and restore the ThreadLocal values
			assertThat(value.get()).isEqualTo("present");
		}

		@Test
		void directMonoSubscribeAsCoreSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();
			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicBoolean complete = new AtomicBoolean();
			CountDownLatch latch = new CountDownLatch(1);

			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			CoreSubscriberWithContext subscriberWithContext =
					new CoreSubscriberWithContext(value, error, latch, complete);

			mono.subscribe(subscriberWithContext);

			latch.await(10, TimeUnit.MILLISECONDS);

			assertThat(error.get()).isNull();
			assertThat(complete.get()).isTrue();

			// We can't do anything here. subscribe(CoreSubscriber) is abstract in
			// CoreSubscriber interface and we have no means to intercept the calls to
			// restore ThreadLocals.
			assertThat(value.get()).isEqualTo("ref_init");
		}

		@Test
		void directMonoSubscribeAsRawSubscriber() throws InterruptedException {
			AtomicReference<String> value = new AtomicReference<>();
			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicBoolean complete = new AtomicBoolean();
			CountDownLatch latch = new CountDownLatch(1);

			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			CoreSubscriberWithContext subscriberWithContext =
					new CoreSubscriberWithContext(value, error, latch, complete);

			// We force the use of subscribe(Subscriber) override instead of
			// subscribe(CoreSubscriber), and we can observe that for such a case we
			// are able to wrap the Subscriber and restore ThreadLocal values for the
			// signals received downstream.
			mono.subscribe((Subscriber<? super String>) subscriberWithContext);

			latch.await(10, TimeUnit.MILLISECONDS);

			assertThat(error.get()).isNull();
			assertThat(complete.get()).isTrue();
			assertThat(value.get()).isEqualTo("present");
		}

		// Flux tests

		@Test
		void fluxIgnoreThenSwitchThread() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Flux.just("Bye").then(mono);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void fluxSwitchThreadThenIgnore() {
			Flux<String> flux = new ThreadSwitchingFlux<>("Ignored", executorService);
			Mono<String> chain = flux.then(Mono.just("Hello"));
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void fluxDeferContextual() {
			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> chain = Flux.deferContextual(ctx -> flux);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void fluxFirstWithSignalArray() {
			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> chain = Flux.firstWithSignal(flux);
			assertThreadLocalPresentInOnNext(chain);

			Flux<String> other = new ThreadSwitchingFlux<>("Hello", executorService);
			assertThreadLocalPresentInOnNext(chain.or(other));
		}

		@Test
		void fluxFirstWithSignalIterable() {
			Flux<String> flux = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> chain = Flux.firstWithSignal(Collections.singletonList(flux));
			assertThreadLocalPresentInOnNext(chain);

			Flux<String> other1 = new ThreadSwitchingFlux<>("Hello", executorService);
			Flux<String> other2 = new ThreadSwitchingFlux<>("Hello", executorService);
			List<Flux<String>> list = Stream.of(other1, other2).collect(Collectors.toList());
			assertThreadLocalPresentInOnNext(Flux.firstWithSignal(list));
		}

		// Mono tests

		@Test
		void monoSwitchThreadIgnoreThen() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = mono.then(Mono.just("Bye"));
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void monoIgnoreThenSwitchThread() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.just("Bye").then(mono);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void monoDeferContextual() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.deferContextual(ctx -> mono);
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void monoFirstWithSignalArray() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.firstWithSignal(mono);
			assertThreadLocalPresentInOnNext(chain);

			Mono<String> other = new ThreadSwitchingMono<>("Hello", executorService);
			assertThreadLocalPresentInOnNext(chain.or(other));
		}

		@Test
		void monoFirstWithSignalIterable() {
			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.firstWithSignal(Collections.singletonList(mono));
			assertThreadLocalPresentInOnNext(chain);

			Mono<String> other1 = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> other2 = new ThreadSwitchingMono<>("Hello", executorService);
			List<Mono<String>> list = Stream.of(other1, other2).collect(Collectors.toList());
			assertThreadLocalPresentInOnNext(Mono.firstWithSignal(list));
		}

		// ParallelFlux tests

		@Test
		void fuseableParallelFluxToMono() {
			Mono<String> flux = new ThreadSwitchingMono<>("Hello", executorService);
			Mono<String> chain = Mono.from(ParallelFlux.from(flux));
			assertThreadLocalPresentInOnNext(chain);
		}

		@Test
		void parallelFlux() {
			AtomicReference<String> value = new AtomicReference<>();

			Mono<String> mono = new ThreadSwitchingMono<>("Hello", executorService);

			ParallelFlux.from(mono)
			            .doOnNext(i -> value.set(REF.get()))
			            .sequential()
			            .contextWrite(Context.of(KEY, "present"))
			            .blockLast();

			assertThat(value.get()).isEqualTo("present");
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

			if (!latch.await(10, TimeUnit.MILLISECONDS)) {
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

		private class CoreSubscriberWithContext implements CoreSubscriber<String> {

			private final AtomicReference<String>    value;
			private final AtomicReference<Throwable> error;
			private final CountDownLatch             latch;
			private final AtomicBoolean              complete;

			public CoreSubscriberWithContext(AtomicReference<String> value,
					AtomicReference<Throwable> error,
					CountDownLatch latch,
					AtomicBoolean complete) {
				this.value = value;
				this.error = error;
				this.latch = latch;
				this.complete = complete;
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
			public void onNext(String s) {
				value.set(REF.get());
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				latch.countDown();
			}

			@Override
			public void onComplete() {
				complete.set(true);
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
}

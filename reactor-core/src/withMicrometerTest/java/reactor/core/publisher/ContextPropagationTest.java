/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.micrometer.context.ContextRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.publisher.FluxHandle.HandleConditionalSubscriber;
import reactor.core.publisher.FluxHandle.HandleSubscriber;
import reactor.core.publisher.FluxHandleFuseable.HandleFuseableConditionalSubscriber;
import reactor.core.publisher.FluxHandleFuseable.HandleFuseableSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.ParameterizedTestWithName;
import reactor.test.subscriber.TestSubscriber;
import reactor.test.subscriber.TestSubscriberBuilder;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class ContextPropagationTest {

	private static final String KEY1 = "ContextPropagationTest.key1";
	private static final String KEY2 = "ContextPropagationTest.key2";

	private static final ThreadLocal<String> REF1 = ThreadLocal.withInitial(() -> "ref1_init");
	private static final ThreadLocal<String> REF2 = ThreadLocal.withInitial(() -> "ref2_init");

	@BeforeAll
	static void initializeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();

		globalRegistry.registerThreadLocalAccessor(KEY1, REF1);
		globalRegistry.registerThreadLocalAccessor(KEY2, REF2);
	}

	//the cleanup of "thread locals" could be especially important if one starts relying on
	//the global registry in tests: it would ensure no TL pollution.
	@AfterEach
	void cleanupThreadLocals() {
		REF1.remove();
		REF2.remove();
	}

	@AfterAll
	static void removeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();

		globalRegistry.removeThreadLocalAccessor(KEY1);
		globalRegistry.removeThreadLocalAccessor(KEY2);

	}

	@Test
	void threadLocalsPresentAfterSubscribeOn() {
		Hooks.enableAutomaticContextPropagation();

		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .subscribeOn(Schedulers.boundedElastic())
		    .doOnNext(i -> tlValue.set(REF1.get()))
		    .contextWrite(Context.of(KEY1, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentAfterPublishOn() {
		Hooks.enableAutomaticContextPropagation();

		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .publishOn(Schedulers.boundedElastic())
		    .doOnNext(i -> tlValue.set(REF1.get()))
		    .contextWrite(Context.of(KEY1, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentInFlatMap() {
		Hooks.enableAutomaticContextPropagation();

		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .flatMap(i -> Mono.just(i)
		                      .doOnNext(j -> tlValue.set(REF1.get())))
		    .contextWrite(Context.of(KEY1, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsPresentAfterDelay() {
		Hooks.enableAutomaticContextPropagation();

		AtomicReference<String> tlValue = new AtomicReference<>();

		Flux.just(1)
		    .delayElements(Duration.ofMillis(1))
		    .doOnNext(i -> tlValue.set(REF1.get()))
		    .contextWrite(Context.of(KEY1, "present"))
		    .blockLast();

		assertThat(tlValue.get()).isEqualTo("present");
	}

	@Test
	void threadLocalsRestoredAfterPollution() {
		// this test validates Queue wrapping takes place
		Hooks.enableAutomaticContextPropagation();
		ArrayBlockingQueue<String> modifiedThreadLocals = new ArrayBlockingQueue<>(10);
		ArrayBlockingQueue<String> restoredThreadLocals = new ArrayBlockingQueue<>(10);

		Flux.range(0, 10)
				.doOnNext(i -> {
					REF1.set("i: " + i);
				})
				.publishOn(Schedulers.parallel())
		        // the validation below shows that modifications to TLs are propagated
		        // across thread boundaries via queue wrapping, so explicit control
		        // is required from users to clean up after such modifications
				.doOnNext(i -> modifiedThreadLocals.add(REF1.get()))
				.contextWrite(Function.identity())
                // the contextWrite above creates a barrier that ensures the downstream
		        // operator sees TLs from the subscriber context
				.doOnNext(i -> restoredThreadLocals.add(REF1.get()))
				.contextWrite(Context.of(KEY1, "present"))
				.blockLast();

		assertThat(modifiedThreadLocals).containsExactly(
				"i: 0", "i: 1", "i: 2", "i: 3", "i: 4",
				"i: 5", "i: 6", "i: 7", "i: 8", "i: 9"
		);
		assertThat(restoredThreadLocals).containsExactly(
				Collections.nCopies(10, "present").toArray(new String[] {})
		);
	}

	@Test
	@SuppressWarnings("unchecked")
	void contextCapturePropagatedAutomaticallyToAllSignals() throws InterruptedException {
		Hooks.enableAutomaticContextPropagation();

		AtomicReference<String> requestTlValue = new AtomicReference<>();
		AtomicReference<String> subscribeTlValue = new AtomicReference<>();
		AtomicReference<String> firstNextTlValue = new AtomicReference<>();
		AtomicReference<String> secondNextTlValue = new AtomicReference<>();
		AtomicReference<String> cancelTlValue = new AtomicReference<>();

		CountDownLatch itemDelivered = new CountDownLatch(1);
		CountDownLatch cancelled = new CountDownLatch(1);

		TestSubscriber<Integer> subscriber =
				TestSubscriber.builder().initialRequest(1).build();

		REF1.set("downstreamContext");

		Flux.just(1, 2)
		    .hide()
		    .doOnRequest(r -> requestTlValue.set(REF1.get()))
		    .doOnNext(i -> firstNextTlValue.set(REF1.get()))
		    .doOnSubscribe(s -> subscribeTlValue.set(REF1.get()))
		    .doOnCancel(() -> {
			    cancelTlValue.set(REF1.get());
			    cancelled.countDown();
		    })
		    .delayElements(Duration.ofMillis(1))
		    .contextWrite(Context.of(KEY1, "upstreamContext"))
		    // disabling prefetching to observe cancellation
		    .publishOn(Schedulers.parallel(), 1)
		    .doOnNext(i -> {
			    System.out.println(REF1.get());
				secondNextTlValue.set(REF1.get());
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
		Hooks.enableAutomaticContextPropagation();

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

		assertThat(REF1.get()).isEqualTo("ref1_init");

		ArrayBlockingQueue<String> innerThreadLocals = new ArrayBlockingQueue<>(size);
		ArrayBlockingQueue<String> outerThreadLocals = new ArrayBlockingQueue<>(size);

		source.publishOn(Schedulers.boundedElastic())
		      .flatMap(i -> Mono.just(i)
		                        .delayElement(Duration.ofMillis(1))
		                        .doOnNext(j -> innerThreadLocals.add(REF1.get())))
		      .contextWrite(ctx -> ctx.put(KEY1, "present"))
		      .publishOn(Schedulers.parallel())
		      .doOnNext(i -> outerThreadLocals.add(REF1.get()))
		      .blockLast();

		assertThat(innerThreadLocals).containsOnly("present").hasSize(size);
		assertThat(outerThreadLocals).containsOnly("ref1_init").hasSize(size);
	}

	@Test
	void isContextPropagationAvailable() {
		assertThat(ContextPropagation.isContextPropagationAvailable()).isTrue();
	}

	@Test
	void contextCaptureWithNoPredicateReturnsTheConstantFunction() {
		assertThat(ContextPropagation.contextCapture())
			.as("no predicate nor registry")
			.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE);
	}

	@Test
	void contextCaptureWithPredicateReturnsNewFunctionWithGlobalRegistry() {
		Function<Context, Context> test = ContextPropagation.contextCapture(ContextPropagation.PREDICATE_TRUE);

		assertThat(test)
			.as("predicate, no registry")
			.isNotNull()
			.isNotSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			.isNotSameAs(ContextPropagation.NO_OP)
			// as long as a predicate is supplied, the method creates new instances of the Function
			.isNotSameAs(ContextPropagation.contextCapture(ContextPropagation.PREDICATE_TRUE));
	}

	@Test
	void fluxApiUsesContextPropagationConstantFunction() {
		Flux<Integer> source = Flux.empty();
		assertThat(source.contextCapture())
			.isInstanceOfSatisfying(FluxContextWrite.class, fcw ->
				assertThat(fcw.doOnContext)
					.as("flux's capture function")
					.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			);
	}

	@Test
	void monoApiUsesContextPropagationConstantFunction() {
		Mono<Integer> source = Mono.empty();
		assertThat(source.contextCapture())
			.isInstanceOfSatisfying(MonoContextWrite.class, fcw ->
				assertThat(fcw.doOnContext)
					.as("mono's capture function")
					.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			);
	}

	@Test
	void fluxApiUsesContextPropagationConstantFunctionWhenAutomaticPropagationEnabled() {
		Hooks.enableAutomaticContextPropagation();
		Flux<Integer> source = Flux.empty();
		assertThat(source.contextCapture())
				.isInstanceOfSatisfying(FluxContextWriteRestoringThreadLocals.class,
						fcw -> assertThat(fcw.doOnContext)
								.as("flux's capture function")
								.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
				);
	}

	@Test
	void monoApiUsesContextPropagationConstantFunctionWhenAutomaticPropagationEnabled() {
		Hooks.enableAutomaticContextPropagation();
		Mono<Integer> source = Mono.empty();
		assertThat(source.contextCapture())
				.isInstanceOfSatisfying(MonoContextWriteRestoringThreadLocals.class,
						fcw -> assertThat(fcw.doOnContext)
								.as("mono's capture function")
								.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE));
	}

	@Nested
	class ContextCaptureFunctionTest {

		@Test
		void contextCaptureFunctionWithoutFiltering() {
			Function<Context, Context> test = ContextPropagation.contextCapture();

			REF1.set("expected1");
			REF2.set("expected2");

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
				.containsEntry(KEY1, "expected1")
				.containsEntry(KEY2, "expected2")
				.hasSize(2);
		}

		@Test
		void captureWithFiltering() {
			Function<Context, Context> test = ContextPropagation.contextCapture(k -> k.toString().equals(KEY2));

			REF1.set("not_expected");
			REF2.set("expected");

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
					.containsEntry(KEY2, "expected")
					.hasSize(1);
		}
	}

	static private enum Cases {
		NORMAL_NO_CONTEXT(false, false, false),
		NORMAL_WITH_CONTEXT(false, false, true),
		CONDITIONAL_NO_CONTEXT(false, true, false),
		CONDITIONAL_WITH_CONTEXT(false, true, true),
		FUSED_NO_CONTEXT(true, false, false),
		FUSED_WITH_CONTEXT(true, false, true),
		FUSED_CONDITIONAL_NO_CONTEXT(true, true, false),
		FUSED_CONDITIONAL_WITH_CONTEXT(true, true, true);

		final boolean fusion;
		final boolean conditional;
		final boolean withContext;

		Cases(boolean fusion, boolean conditional, boolean withContext) {
			this.fusion = fusion;
			this.conditional = conditional;
			this.withContext = withContext;
		}
	}

	@Nested
	class ContextRestoreForTap {

		@EnumSource(Cases.class)
		@ParameterizedTestWithName
		void properWrappingForFluxTap(Cases characteristics) {
			SignalListener<String> originalListener = Mockito.mock(SignalListener.class);
			SignalListenerFactory<String, Void> originalFactory = new SignalListenerFactory<String, Void>() {
				@Override
				public Void initializePublisherState(Publisher<? extends String> source) {
					return null;
				}

				@Override
				public SignalListener<String> createListener(Publisher<? extends String> source,
															 ContextView listenerContext, Void publisherContext) {
					return originalListener;
				}
			};

			Publisher<String> tap;
			TestSubscriberBuilder builder = TestSubscriber.builder();
			if (characteristics.fusion) {
				tap = new FluxTapFuseable<>(Flux.empty(), originalFactory);
				builder = builder.requireFusion(Fuseable.ANY);
			}
			else {
				tap = new FluxTap<>(Flux.empty(), originalFactory);
				builder = builder.requireNotFuseable();
			}

			if (characteristics.withContext) {
				builder = builder.contextPut("properWrappingForFluxTap", true);
			}

			TestSubscriber<String> testSubscriber;
			if (characteristics.conditional) {
				testSubscriber = builder.buildConditional(v -> true);
			}
			else {
				testSubscriber = builder.build();
			}

			tap.subscribe(testSubscriber);
			Scannable parent = testSubscriber.parents().findFirst().get();

			if (!characteristics.fusion) {
				assertThat(parent).isInstanceOfSatisfying(FluxTap.TapSubscriber.class,
					tapSubscriber -> {
						if (characteristics.withContext) {
							assertThat(tapSubscriber.listener).as("listener wrapped")
								.isNotSameAs(originalListener)
								.isInstanceOf(ContextPropagation.ContextRestoreSignalListener.class);
						}
						else {
							assertThat(tapSubscriber.listener)
								.as("listener not wrapped")
								.isSameAs(originalListener);
						}
					});
			}
			else {
				assertThat(parent).isInstanceOfSatisfying(FluxTapFuseable.TapFuseableSubscriber.class,
					tapSubscriber -> {
						if (characteristics.withContext) {
							assertThat(tapSubscriber.listener)
								.as("listener wrapped")
								.isNotSameAs(originalListener)
								.isInstanceOf(ContextPropagation.ContextRestoreSignalListener.class);
						}
						else {
							assertThat(tapSubscriber.listener)
								.as("listener not wrapped")
								.isSameAs(originalListener);
						}
					});
			}
		}

		@EnumSource(Cases.class)
		@ParameterizedTestWithName
		void properWrappingForMonoTap(Cases characteristics) {
			SignalListener<String> originalListener = Mockito.mock(SignalListener.class);
			SignalListenerFactory<String, Void> originalFactory = new SignalListenerFactory<String, Void>() {
				@Override
				public Void initializePublisherState(Publisher<? extends String> source) {
					return null;
				}

				@Override
				public SignalListener<String> createListener(Publisher<? extends String> source,
															 ContextView listenerContext, Void publisherContext) {
					return originalListener;
				}
			};

			Mono<String> tap;
			TestSubscriberBuilder builder = TestSubscriber.builder();
			if (characteristics.fusion) {
				tap = new MonoTapFuseable<>(Mono.empty(), originalFactory);
				builder = builder.requireFusion(Fuseable.ANY);
			}
			else {
				tap = new MonoTap<>(Mono.empty(), originalFactory);
				builder = builder.requireNotFuseable();
			}

			if (characteristics.withContext) {
				builder = builder.contextPut("properWrappingForMonoTap", true);
			}

			TestSubscriber<String> testSubscriber;
			if (characteristics.conditional) {
				testSubscriber = builder.buildConditional(v -> true);
			}
			else {
				testSubscriber = builder.build();
			}

			tap.subscribe(testSubscriber);
			Scannable parent = testSubscriber.parents().findFirst().get();

			if (!characteristics.fusion) {
				assertThat(parent).isInstanceOfSatisfying(FluxTap.TapSubscriber.class,
					tapSubscriber -> {
						if (characteristics.withContext) {
							assertThat(tapSubscriber.listener)
								.as("listener wrapped")
								.isNotSameAs(originalListener)
								.isInstanceOf(ContextPropagation.ContextRestoreSignalListener.class);

						}
						else {
							assertThat(tapSubscriber.listener).as("listener not wrapped").isSameAs(originalListener);
						}
					});
			}
			else {
				assertThat(parent).isInstanceOfSatisfying(FluxTapFuseable.TapFuseableSubscriber.class,
					tapSubscriber -> {
						if (characteristics.withContext) {
							assertThat(tapSubscriber.listener)
								.as("listener wrapped")
								.isNotSameAs(originalListener)
								.isInstanceOf(ContextPropagation.ContextRestoreSignalListener.class);
						}
						else {
							assertThat(tapSubscriber.listener).as("listener not wrapped").isSameAs(originalListener);
						}
					});
			}
		}

		@Test
		void threadLocalRestoredInSignalListener() throws InterruptedException {
			REF1.set(null);
			Context context = Context.of(KEY1, "expected");
			List<String> list = new ArrayList<>();

			SignalListener<Object> tlReadingListener = Mockito.mock(SignalListener.class, invocation -> {
				list.add(invocation.getMethod().getName() + ": " + REF1.get());
				return null;
			});

			ContextPropagation.ContextRestoreSignalListener<Object> listener =
				new ContextPropagation.ContextRestoreSignalListener<>(tlReadingListener, context,
				null);

			Thread t = new Thread(() -> {
				try {
					listener.doFirst();
					listener.doOnSubscription();

					listener.doOnFusion(1);
					listener.doOnRequest(1L);
					listener.doOnCancel();

					listener.doOnNext(1);
					listener.doOnComplete();
					listener.doOnError(new IllegalStateException("boom"));

					listener.doAfterComplete();
					listener.doAfterError(new IllegalStateException("boom"));
					listener.doFinally(SignalType.ON_COMPLETE);

					listener.doOnMalformedOnNext(1);
					listener.doOnMalformedOnComplete();
					listener.doOnMalformedOnError(new IllegalStateException("boom"));

					listener.addToContext(Context.empty());
					listener.handleListenerError(new IllegalStateException("boom"));
				}
				catch (Throwable error) {
					error.printStackTrace();
				}
			});
			t.start();
			t.join();

			assertThat(list).as("extracted TLs")
				.containsExactly(
					"doFirst: expected",
					"doOnSubscription: expected",
					"doOnFusion: expected",
					"doOnRequest: expected",
					"doOnCancel: expected",
					"doOnNext: expected",
					"doOnComplete: expected",
					"doOnError: expected",
					"doAfterComplete: expected",
					"doAfterError: expected",
					"doFinally: expected",
					"doOnMalformedOnNext: expected",
					"doOnMalformedOnComplete: expected",
					"doOnMalformedOnError: expected",
					"addToContext: expected",
					"handleListenerError: expected"
				);
		}

	}

	@Nested
	class ContextRestoreForHandle {

		@ValueSource(booleans = {true, false})
		@ParameterizedTestWithName
		void publicMethodChecksForContextNotEmptyBeforeWrapping(boolean withContext) {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> { };
			final Context context;
			if (withContext) {
				context = Context.of(KEY1, "expected");
			}
			else {
				context = Context.empty();
			}

			BiConsumer<String, SynchronousSink<String>> decoratedHandler = ContextPropagation.contextRestoreForHandle(originalHandler,
				() -> context);

			if (withContext) {
				assertThat(decoratedHandler).as("context not empty: decorated handler").isNotSameAs(originalHandler);
			}
			else {
				assertThat(decoratedHandler).as("empty context: same handler").isSameAs(originalHandler);
			}
		}

		@Test
		void classContextRestoreHandleConsumerRestoresThreadLocal() {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> {
				if (v.equals("bar")) {
					sink.next(v + "=" + REF1.get());
				}
			};

			final String expected = "bar=expected";
			final Context context = Context.of(KEY1, "expected");

			BiConsumer<String, SynchronousSink<String>> decoratedHandler =
				ContextPropagation.contextRestoreForHandle(originalHandler, () -> context);

			SynchronousSink<String> mockSink = Mockito.mock(SynchronousSink.class);
			decoratedHandler.accept("bar", mockSink);
			Mockito.verify(mockSink, Mockito.times(1)).next(expected);
		}

		@SuppressWarnings("rawtypes")
		@Test
		void fluxHandleVariantsCallTheWrapper() {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> {};

			FluxHandle<String, String> publisher = new FluxHandle<>(Flux.empty(), originalHandler);
			FluxHandleFuseable<String, String> publisherFuseable = new FluxHandleFuseable<>(Flux.empty(), originalHandler);

			CoreSubscriber<Object> actual = TestSubscriber.builder().contextPut("fluxHandleVariantsCallTheWrapper", true).build();
			Fuseable.ConditionalSubscriber<Object> actualCondi = TestSubscriber.builder().contextPut("fluxHandleVariantsCallTheWrapper", true).buildConditional(v -> true);

			HandleSubscriber sub = (HandleSubscriber) publisher.subscribeOrReturn(actual);
			HandleConditionalSubscriber subCondi = (HandleConditionalSubscriber) publisher.subscribeOrReturn(actualCondi);
			HandleFuseableSubscriber subFused = (HandleFuseableSubscriber) publisherFuseable.subscribeOrReturn(actual);
			HandleFuseableConditionalSubscriber subFusedCondi = (HandleFuseableConditionalSubscriber) publisherFuseable.subscribeOrReturn(actualCondi);

			SoftAssertions.assertSoftly(softly -> {
				softly.assertThat(publisher.handler).as("publisher.handler").isSameAs(originalHandler);
				softly.assertThat(publisherFuseable.handler).as("publisherFuseable.handler").isSameAs(originalHandler);

				softly.assertThat(sub.handler)
					.as("sub.handler")
					.isNotSameAs(originalHandler);
				softly.assertThat(subCondi.handler)
					.as("subCondi.handler")
					.isNotSameAs(originalHandler);
				softly.assertThat(subFused.handler)
					.as("subFused.handler")
					.isNotSameAs(originalHandler);
				softly.assertThat(subFusedCondi.handler)
					.as("subFusedCondi.handler")
					.isNotSameAs(originalHandler);
			});
		}

		@SuppressWarnings("rawtypes")
		@Test
		void monoHandleVariantsCallTheWrapper() {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> {};

			MonoHandle<String, String> publisher = new MonoHandle<>(Mono.empty(), originalHandler);
			MonoHandleFuseable<String, String> publisherFuseable = new MonoHandleFuseable<>(Mono.empty(), originalHandler);

			CoreSubscriber<Object> actual = TestSubscriber.builder().contextPut("monoHandleVariantsCallTheWrapper", true).build();

			HandleSubscriber sub = (HandleSubscriber) publisher.subscribeOrReturn(actual);
			HandleFuseableSubscriber subFused = (HandleFuseableSubscriber) publisherFuseable.subscribeOrReturn(actual);
			//note: unlike FluxHandle, MonoHandle doesn't have support for ConditionalSubscriber

			SoftAssertions.assertSoftly(softly -> {
				softly.assertThat(publisher.handler).as("publisher.handler").isSameAs(originalHandler);
				softly.assertThat(publisherFuseable.handler).as("publisherFuseable.handler").isSameAs(originalHandler);

				softly.assertThat(sub.handler)
				      .as("sub.handler")
				      .isNotSameAs(originalHandler);
				softly.assertThat(subFused.handler)
				      .as("subFused.handler")
				      .isNotSameAs(originalHandler);
			});
		}
	}
}

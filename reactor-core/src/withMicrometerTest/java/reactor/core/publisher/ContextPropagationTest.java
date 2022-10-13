/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.micrometer.context.ContextRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import reactor.test.ParameterizedTestWithName;
import reactor.test.subscriber.TestSubscriber;
import reactor.test.subscriber.TestSubscriberBuilder;
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

	//NOTE: no way to currently remove accessors from the ContextRegistry, so we recreate one on each test
	private ContextRegistry registry;

	@BeforeEach
	void initializeThreadLocals() {
		registry = new ContextRegistry().loadContextAccessors();

		registry.registerThreadLocalAccessor(KEY1, REF1);
		registry.registerThreadLocalAccessor(KEY2, REF2);
	}

	//the cleanup of "thread locals" could be especially important if one starts relying on
	//the global registry in tests: it would ensure no TL pollution.
	@AfterEach
	void cleanupThreadLocals() {
		REF1.remove();
		REF2.remove();
	}

	@Test
	void isContextPropagationAvailable() {
		assertThat(ContextPropagation.isContextPropagationAvailable()).isTrue();
	}


	@Test
	void contextCaptureWithNoPredicateReturnsTheConstantFunction() {
		assertThat(ContextPropagation.contextCapture())
			.as("no predicate nor registry")
			.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			.hasFieldOrPropertyWithValue("registry", ContextRegistry.getInstance());
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
			.isNotSameAs(ContextPropagation.contextCapture(ContextPropagation.PREDICATE_TRUE))
			.isInstanceOfSatisfying(ContextPropagation.ContextCaptureFunction.class, f ->
				assertThat(f.registry).as("function default registry").isSameAs(ContextRegistry.getInstance()));
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

	@Nested
	class ContextCaptureFunctionTest {

		@Test
		void contextCaptureFunctionWithoutFiltering() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(
				ContextPropagation.PREDICATE_TRUE, registry);

			REF1.set("expected1");
			REF2.set("expected2");

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
				.containsEntry(KEY1, "expected1")
				.containsEntry(KEY2, "expected2")
				.containsEntry(ContextPropagation.CAPTURED_CONTEXT_MARKER, true)
				.hasSize(3);
		}

		@Test
		void captureWithFiltering() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(
				k -> k.toString().equals(KEY2), registry);

			REF1.set("not_expected");
			REF2.set("expected");

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
				.containsEntry(KEY2, "expected")
				.containsEntry(ContextPropagation.CAPTURED_CONTEXT_MARKER, true)
				.hasSize(2);
		}

		@Test
		void captureFunctionWithNullRegistryUsesGlobalRegistry() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(v -> true, null);

			assertThat(test.registry).as("default registry").isSameAs(ContextRegistry.getInstance());
		}
	}

	static private enum Cases {
		NORMAL_NO_MARKER(false, false, false),
		NORMAL_WITH_MARKER(false, false, true),
		CONDITIONAL_NO_MARKER(false, true, false),
		CONDITIONAL_WITH_MARKER(false, true, true),
		FUSED_NO_MARKER(true, false, false),
		FUSED_WITH_MARKER(true, false, true),
		FUSED_CONDITIONAL_NO_MARKER(true, true, false),
		FUSED_CONDITIONAL_WITH_MARKER(true, true, true);

		final boolean fusion;
		final boolean conditional;
		final boolean marker;

		Cases(boolean fusion, boolean conditional, boolean marker) {
			this.fusion = fusion;
			this.conditional = conditional;
			this.marker = marker;
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

			if (characteristics.marker) {
				builder = builder.contextPut(ContextPropagation.CAPTURED_CONTEXT_MARKER, true);
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
						if (characteristics.marker) {
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
						if (characteristics.marker) {
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

			if (characteristics.marker) {
				builder = builder.contextPut(ContextPropagation.CAPTURED_CONTEXT_MARKER, true);
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
						if (characteristics.marker) {
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
						if (characteristics.marker) {
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

			ContextPropagation.ContextRestoreSignalListener<Object> listener = new ContextPropagation.ContextRestoreSignalListener<>(tlReadingListener, context,
				registry);

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
		void publicMethodChecksForMarkerBeforeWrapping(boolean withMarker) {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> { };
			final Context context;
			if (withMarker) {
				context = Context.of(KEY1, "expected", ContextPropagation.CAPTURED_CONTEXT_MARKER, true);
			}
			else {
				context = Context.of(KEY1, "expected");
			}
			CoreSubscriber<String> mockSubscriber = Mockito.mock(CoreSubscriber.class);
			Mockito.when(mockSubscriber.currentContext()).thenReturn(context);

			BiConsumer<String, SynchronousSink<String>> decoratedHandler = ContextPropagation.contextRestoreForHandle(originalHandler, mockSubscriber);

			if (!withMarker) {
				assertThat(decoratedHandler).as("no marker: same handler").isSameAs(originalHandler);
			}
			else {
				assertThat(decoratedHandler).as("marker: decorated handler").isNotSameAs(originalHandler);
			}
		}

		@ValueSource(booleans = {true, false})
		@ParameterizedTestWithName
		void classContextRestoreHandleConsumerRestoresWithOrWithoutMarker(boolean withMarker) {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> {
				if (v.equals("bar")) {
					sink.next(v + "=" + REF1.get());
				}
			};

			final String expected = "bar=expected";
			final Context context;
			if (withMarker) {
				context = Context.of(KEY1, "expected", ContextPropagation.CAPTURED_CONTEXT_MARKER, true);
			}
			else {
				context = Context.of(KEY1, "expected");
			}

			BiConsumer<String, SynchronousSink<String>> decoratedHandler = new ContextPropagation.ContextRestoreHandleConsumer<>(originalHandler, registry, context);

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

			CoreSubscriber<Object> actual = TestSubscriber.builder().contextPut(ContextPropagation.CAPTURED_CONTEXT_MARKER, true).build();
			Fuseable.ConditionalSubscriber<Object> actualCondi = TestSubscriber.builder().contextPut(ContextPropagation.CAPTURED_CONTEXT_MARKER, true).buildConditional(v -> true);

			HandleSubscriber sub = (HandleSubscriber) publisher.subscribeOrReturn(actual);
			HandleConditionalSubscriber subCondi = (HandleConditionalSubscriber) publisher.subscribeOrReturn(actualCondi);
			HandleFuseableSubscriber subFused = (HandleFuseableSubscriber) publisherFuseable.subscribeOrReturn(actual);
			HandleFuseableConditionalSubscriber subFusedCondi = (HandleFuseableConditionalSubscriber) publisherFuseable.subscribeOrReturn(actualCondi);

			SoftAssertions.assertSoftly(softly -> {
				softly.assertThat(publisher.handler).as("publisher.handler").isSameAs(originalHandler);
				softly.assertThat(publisherFuseable.handler).as("publisherFuseable.handler").isSameAs(originalHandler);

				softly.assertThat(sub.handler)
					.as("sub.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
				softly.assertThat(subCondi.handler)
					.as("subCondi.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
				softly.assertThat(subFused.handler)
					.as("subFused.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
				softly.assertThat(subFusedCondi.handler)
					.as("subFusedCondi.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
			});
		}

		@SuppressWarnings("rawtypes")
		@Test
		void monoHandleVariantsCallTheWrapper() {
			BiConsumer<String, SynchronousSink<String>> originalHandler = (v, sink) -> {};

			MonoHandle<String, String> publisher = new MonoHandle<>(Mono.empty(), originalHandler);
			MonoHandleFuseable<String, String> publisherFuseable = new MonoHandleFuseable<>(Mono.empty(), originalHandler);

			CoreSubscriber<Object> actual = TestSubscriber.builder().contextPut(ContextPropagation.CAPTURED_CONTEXT_MARKER, true).build();

			HandleSubscriber sub = (HandleSubscriber) publisher.subscribeOrReturn(actual);
			HandleFuseableSubscriber subFused = (HandleFuseableSubscriber) publisherFuseable.subscribeOrReturn(actual);
			//note: unlike FluxHandle, MonoHandle doesn't have support for ConditionalSubscriber

			SoftAssertions.assertSoftly(softly -> {
				softly.assertThat(publisher.handler).as("publisher.handler").isSameAs(originalHandler);
				softly.assertThat(publisherFuseable.handler).as("publisherFuseable.handler").isSameAs(originalHandler);

				softly.assertThat(sub.handler)
					.as("sub.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
				softly.assertThat(subFused.handler)
					.as("subFused.handler")
					.isNotSameAs(originalHandler)
					.isInstanceOf(ContextPropagation.ContextRestoreHandleConsumer.class);
			});
		}
	}
}

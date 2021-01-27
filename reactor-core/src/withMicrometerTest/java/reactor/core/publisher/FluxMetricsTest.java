/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.Metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.FluxMetrics.*;
import static reactor.core.publisher.FluxMetrics.TAG_ON_COMPLETE_EMPTY;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;

public class FluxMetricsTest {

	private MeterRegistry registry;
	private MeterRegistry previousRegistry;

	@BeforeEach
	public void setupRegistry() {
		registry = new SimpleMeterRegistry();
		previousRegistry = Metrics.MicrometerConfiguration.useRegistry(registry);
	}

	@AfterEach
	public void removeRegistry() {
		registry.close();
		Metrics.MicrometerConfiguration.useRegistry(previousRegistry);
	}

	@Test
	public void sequenceNameFromScanUnavailable() {
		Flux<String> delegate = Flux.just("foo");
		Flux<String> source = new Flux<String>() {

			@Override
			public void subscribe(CoreSubscriber<? super String> actual) {
				delegate.subscribe(actual);
			}
		};
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan unavailable").isFalse();
		assertThat(test.name).isEqualTo(REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableNoName() {
		Flux<String> source = Flux.just("foo");
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo(REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableName() {
		Flux<String> source = Flux.just("foo").name("foo").hide().hide();
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo("foo");
	}

	@Test
	public void testUsesMicrometer() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new FluxMetrics<>(Flux.just("foo").hide())
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(MetricsSubscriber.class);
	}

	@Test
	public void splitMetricsOnName() {
		final Flux<Integer> unnamedSource = Flux.<Integer>error(new ArithmeticException("boom"))
				.hide();
		final Flux<Integer> unnamed = new FluxMetrics<>(unnamedSource)
				.onErrorResume(e -> Mono.empty());

		final Flux<Integer> namedSource = Flux.range(1, 40)
		                                      .name("foo")
		                                      .map(i -> 100 / (40 - i))
		                                      .hide();
		final Flux<Integer> named = new FluxMetrics<>(namedSource)
				.onErrorResume(e -> Mono.empty());

		Mono.when(unnamed, named).block();

		Timer unnamedMeter = registry
				.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_KEY_EXCEPTION, ArithmeticException.class.getName())
				.timer();

		Timer namedMeter = registry
				.find("foo" + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_KEY_EXCEPTION, ArithmeticException.class.getName())
				.timer();

		assertThat(unnamedMeter).isNotNull();
		assertThat(namedMeter).isNotNull();

		assertThat(unnamedMeter.count()).isOne();
		assertThat(namedMeter.count()).isOne();
	}

	@Test
	public void usesTags() {
		Flux<Integer> source = Flux.range(1, 8)
		                           .tag("tag1", "A")
		                           .name("usesTags")
		                           .tag("tag2", "foo")
		                           .hide();

		new FluxMetrics<>(source).blockLast();

		Timer meter = registry
				.find("usesTags" + METER_ON_NEXT_DELAY)
				.tag("tag1", "A")
				.tag("tag2", "foo")
				.timer();

		assertThat(meter).isNotNull();
		assertThat(meter.count()).isEqualTo(8L);
	}

	@Test
	public void onNextTimerCounts() {
		Flux<Integer> source = Flux.range(1, 123)
		                           .hide();

		new FluxMetrics<>(source).blockLast();

		Timer nextMeter = registry
				.find(REACTOR_DEFAULT_NAME + METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNotNull();
		assertThat(nextMeter.count()).isEqualTo(123L);

		Flux<Integer> source2 = Flux.range(1, 10)
		                            .hide();

		new FluxMetrics<>(source2)
				.take(3)
				.blockLast();

		assertThat(nextMeter.count()).isEqualTo(126L);

		Flux<Integer> source3 = Flux.range(1, 1000)
		                            .name("foo")
		                            .hide();

		new FluxMetrics<>(source3).blockLast();

		assertThat(nextMeter.count())
				.as("notTakingNamedIntoAccount")
				.isEqualTo(126L);
	}

	@Test
	public void malformedOnNext() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
		Flux<Integer> source = testPublisher.flux().hide();

		new FluxMetrics<>(source).subscribe();

		testPublisher.next(1)
		             .complete()
		             .next(2);

		Counter malformedMeter = registry
				.find(REACTOR_DEFAULT_NAME + METER_MALFORMED)
				.counter();

		assertThat(malformedMeter).isNotNull();
		assertThat(malformedMeter.count()).isEqualTo(1);
	}

	@Test
	public void malformedOnError() {
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		Hooks.onErrorDropped(errorDropped::set);
		Exception dropError = new IllegalStateException("malformedOnError");
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
		Flux<Integer> source = testPublisher.flux().hide();

		new FluxMetrics<>(source).subscribe();

		testPublisher.next(1)
		             .complete()
		             .error(dropError);

		Counter malformedMeter = registry
				.find(REACTOR_DEFAULT_NAME + METER_MALFORMED)
				.counter();

		assertThat(malformedMeter).isNotNull();
		assertThat(malformedMeter.count()).isEqualTo(1);
		assertThat(errorDropped).hasValue(dropError);
	}

	@Test
	public void completeEmpty() {
		Flux<Integer> source = Flux.empty();

		new FluxMetrics<>(source).blockLast();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNull();

		assertThat(stcCompleteEmptyCounter.count())
				.as("complete without any element")
				.isOne();
	}

	@Test
	public void completeWithElement() {
		Flux<Integer> source = Flux.just(1, 2, 3);

		new FluxMetrics<>(source).blockLast();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter.count())
				.as("complete with element")
				.isOne();

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNull();
	}

	@Test
	public void subscribeToComplete() {
		Flux<String> source = Flux.just("foo")
		                          .delayElements(Duration.ofMillis(100))
		                          .hide();

		new FluxMetrics<>(source).blockLast();

		Timer stcCompleteTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                               .tags(Tags.of(TAG_CANCEL))
		                               .timer();

		SoftAssertions.assertSoftly(softly -> {
			softly.assertThat(stcCompleteTimer.max(TimeUnit.MILLISECONDS))
					.as("subscribe to complete timer")
					.isGreaterThanOrEqualTo(100);

			softly.assertThat(stcErrorTimer)
					.as("subscribe to error timer is lazily registered")
					.isNull();

			softly.assertThat(stcCancelTimer)
					.as("subscribe to cancel timer")
					.isNull();
		});
	}

	@Test
	public void subscribeToError() {
		Flux<Integer> source = Flux.just(1, 0)
		                           .delayElements(Duration.ofMillis(100))
		                           .map(v -> 100 / v)
		                           .hide();

		new FluxMetrics<>(source)
				.onErrorReturn(-1)
				.blockLast();

		Timer stcCompleteTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                               .tags(Tags.of(TAG_CANCEL))
		                               .timer();

		SoftAssertions.assertSoftly(softly -> {
			softly.assertThat(stcCompleteTimer)
					.as("subscribe to complete timer")
					.isNull();

			softly.assertThat(stcErrorTimer.max(TimeUnit.MILLISECONDS))
					.as("subscribe to error timer")
					.isGreaterThanOrEqualTo(100);

			softly.assertThat(stcCancelTimer)
					.as("subscribe to cancel timer")
					.isNull();
		});
	}

	@Test
	public void subscribeToCancel() {
		Flux<Integer> source = Flux.just(1, 0)
		                           .delayElements(Duration.ofMillis(100))
		                           .hide();

		new FluxMetrics<>(source)
				.take(1)
				.blockLast();

		Timer stcCompleteTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                               .tags(Tags.of(TAG_CANCEL))
		                               .timer();

		SoftAssertions.assertSoftly(softly -> {
			softly.assertThat(stcCompleteTimer)
					.as("subscribe to complete timer")
					.isNull();

			softly.assertThat(stcErrorTimer)
					.as("subscribe to error timer is lazily registered")
					.isNull();

			softly.assertThat(stcCancelTimer.max(TimeUnit.MILLISECONDS))
					.as("subscribe to cancel timer")
					.isGreaterThanOrEqualTo(100);
		});
	}

	@Test
	public void countsSubscriptions() {
		Flux<Integer> source = Flux.range(1, 10)
		                           .hide();

		Flux<Integer> test = new FluxMetrics<>(source);

		test.subscribe();
		Counter meter = registry.find(REACTOR_DEFAULT_NAME + METER_SUBSCRIBED)
		                        .counter();

		assertThat(meter).isNotNull();
		assertThat(meter.count()).as("after 1s subscribe").isEqualTo(1);

		test.subscribe();
		test.subscribe();

		assertThat(meter.count()).as("after more subscribe").isEqualTo(3);
	}

	@Test
	public void requestTrackingDisabledIfNotNamed() {
		Flux<Integer> source = Flux.range(1, 10)
		                           .hide();

		new FluxMetrics<>(source).blockLast();

		DistributionSummary meter = registry.find(REACTOR_DEFAULT_NAME + METER_REQUESTED)
		                                    .summary();

		if (meter != null) { //meter could be null in some tests
			assertThat(meter.count()).isZero();
		}
	}

	@Test
	public void requestTrackingHasMeterForNamedSequence() {
		Flux<Integer> source = Flux.range(1, 10)
		                           .name("foo")
		                           .hide();

		new FluxMetrics<>(source).blockLast();

		DistributionSummary meter = registry.find("foo" + METER_REQUESTED)
		                                    .summary();

		assertThat(meter).as("global find").isNotNull();

		meter = registry.find("foo" + METER_REQUESTED)
		                .summary();

		assertThat(meter).as("tagged find").isNotNull();
	}

	@Test
	public void requestTracking() {
		BaseSubscriber<Integer> bs = new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				subscription.request(1);
			}
		};
		Flux<Integer> source = Flux.range(1, 10)
		                           .name("foo")
		                           .hide();

		new FluxMetrics<>(source).subscribe(bs);

		DistributionSummary meter = registry.find("foo" + METER_REQUESTED)
		                                    .summary();

		assertThat(meter).as("meter").isNotNull();
		assertThat(meter.totalAmount()).isEqualTo(1);

		bs.request(7);
		assertThat(meter.totalAmount()).isEqualTo(8);
		assertThat(meter.max()).isEqualTo(7);

		bs.request(100);
		assertThat(meter.totalAmount()).isEqualTo(108);
		assertThat(meter.max()).isEqualTo(100);
	}

	//see https://github.com/reactor/reactor-core/issues/1425
	@Test
	public void flowDurationTagsConsistency() {
		Flux<Integer> source1 = Flux.just(1)
		                            .hide();

		Flux<Object> source2 = Flux.error(new IllegalStateException("dummy"))
		                           .hide();

		new FluxMetrics<>(source1)
				.blockLast();

		new FluxMetrics<>(source2)
				.onErrorReturn(0)
				.blockLast();

		Set<Set<String>> uniqueTagKeySets = registry
				.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.meters()
				.stream()
				.map(it -> it.getId().getTags())
				.map(it -> it.stream().map(Tag::getKey).collect(Collectors.toSet()))
				.collect(Collectors.toSet());

		assertThat(uniqueTagKeySets).hasSize(1);
	}

	@Test
	public void ensureFuseablePropagateOnComplete_inCaseOfAsyncFusion() {
		Flux<Integer> source = Flux.fromIterable(Arrays.asList(1, 2, 3));
		//smoke test that this form uses FluxMetricsFuseable
		assertThat(source.metrics()).isInstanceOf(FluxMetricsFuseable.class);

		//now use the test version with local registry
		new FluxMetricsFuseable<Integer>(source)
		    .flatMapIterable(Arrays::asList)
		    .as(StepVerifier::create)
		    .expectNext(1, 2, 3)
		    .expectComplete()
		    .verify(Duration.ofMillis(500));
	}

	@Test
	public void ensureOnNextInAsyncModeIsCapableToPropagateNulls() {
		Flux<List<Integer>> source = Flux.using(() -> "irrelevant",
				irrelevant -> Mono.fromSupplier(() -> Arrays.asList(1, 2, 3)),
				irrelevant -> {
				});

		//smoke test that this form uses FluxMetricsFuseable
		assertThat(source.metrics()).isInstanceOf(FluxMetricsFuseable.class);

		//now use the test version with local registry
		new FluxMetricsFuseable<List<Integer>>(source)
		    .flatMapIterable(Function.identity())
		    .as(StepVerifier::create)
		    .expectNext(1, 2, 3)
		    .expectComplete()
		    .verify(Duration.ofMillis(500));
	}

	@Test
	void ensureMetricsUsesTheTagValueClosestToItWhenCalledMultipleTimes() {
		Flux<Integer> level1 = new FluxMetrics<>(
				Flux.range(1, 10)
				    .name("pipelineFlux")
				    .tag("operation", "rangeFlux"));

		Flux<String> level2 = new FluxMetricsFuseable<>(
				level1.map(Object::toString)
				      .name("pipelineFlux")
				      .tag("operation", "mapFlux"));

		Flux<String> level3 = new FluxMetrics<>(
				level2.filter(i -> i.length() > 3)
				      .name("pipelineFlux")
				      .tag("operation", "filterFlux"));

		level3.blockLast();

		Collection<Meter> meters = registry
				.find("pipelineFlux" + METER_FLOW_DURATION)
				.tagKeys("operation")
				.meters();

		assertThat(meters)
				.isNotEmpty()
				.extracting(m -> m.getId().getTag("operation"))
				.containsExactlyInAnyOrder("rangeFlux", "mapFlux", "filterFlux");
	}
}

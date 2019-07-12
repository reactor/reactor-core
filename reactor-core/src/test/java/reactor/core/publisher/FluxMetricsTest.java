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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.FluxMetrics.*;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;

public class FluxMetricsTest {

	private MeterRegistry registry;

	@Before
	public void setupRegistry() {
		registry = new SimpleMeterRegistry();
	}

	@After
	public void removeRegistry() {
		registry.close();
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
		FluxMetrics<String> test = new FluxMetrics<>(source, registry);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan unavailable").isFalse();
		assertThat(test.name).isEqualTo(REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableNoName() {
		Flux<String> source = Flux.just("foo");
		FluxMetrics<String> test = new FluxMetrics<>(source, registry);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo(REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableName() {
		Flux<String> source = Flux.just("foo").name("foo").hide().hide();
		FluxMetrics<String> test = new FluxMetrics<>(source, registry);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo("foo");
	}

	@Test
	public void testUsesMicrometer() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new FluxMetrics<>(Flux.just("foo").hide(), registry)
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(MetricsSubscriber.class);
	}

	@Test
	public void splitMetricsOnName() {
		final Flux<Integer> unnamedSource = Flux.<Integer>error(new ArithmeticException("boom"))
				.hide();
		final Flux<Integer> unnamed = new FluxMetrics<>(unnamedSource, registry)
				.onErrorResume(e -> Mono.empty());
		final Flux<Integer> namedSource = Flux.range(1, 40)
		                                      .name("foo")
		                                      .map(i -> 100 / (40 - i))
		                                      .hide();
		final Flux<Integer> named = new FluxMetrics<>(namedSource, registry)
				.onErrorResume(e -> Mono.empty());

		Mono.when(unnamed, named).block();

		Timer unnamedMeter = registry
				.find(METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_KEY_EXCEPTION, ArithmeticException.class.getName())
				.tag(TAG_SEQUENCE_NAME, REACTOR_DEFAULT_NAME)
				.timer();

		Timer namedMeter = registry
				.find(METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_KEY_EXCEPTION, ArithmeticException.class.getName())
				.tag(TAG_SEQUENCE_NAME, "foo")
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
		new FluxMetrics<>(source, registry)
				.blockLast();

		Timer meter = registry
				.find(METER_ON_NEXT_DELAY)
				.tag(TAG_SEQUENCE_NAME, "usesTags")
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
		new FluxMetrics<>(source, registry)
				.blockLast();

		Timer nextMeter = registry
				.find(METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNotNull();
		assertThat(nextMeter.count()).isEqualTo(123L);

		Flux<Integer> source2 = Flux.range(1, 10)
		                            .hide();
		new FluxMetrics<>(source2, registry)
				.take(3)
				.blockLast();

		assertThat(nextMeter.count()).isEqualTo(126L);

		Flux<Integer> source3 = Flux.range(1, 1000)
		                            .name("foo")
		                            .hide();
		new FluxMetrics<>(source3, registry)
				.blockLast();

		assertThat(nextMeter.count())
				.as("notTakingNamedIntoAccount")
				.isEqualTo(126L);
	}

	@Test
	public void malformedOnNext() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
		Flux<Integer> source = testPublisher.flux().hide();

		new FluxMetrics<>(source, registry)
				.subscribe();

		testPublisher.next(1)
		             .complete()
		             .next(2);

		Counter malformedMeter = registry
				.find(METER_MALFORMED)
				.counter();

		assertThat(malformedMeter).isNotNull();
		assertThat(malformedMeter.count()).isEqualTo(1);
	}

	@Test
	public void malformedOnError() {
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		Hooks.onErrorDropped(errorDropped::set);
		Exception dropError = new IllegalStateException("malformedOnError");
		try {
			TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
			Flux<Integer> source = testPublisher.flux().hide();

			new FluxMetrics<>(source, registry)
					.subscribe();

			testPublisher.next(1)
			             .complete()
			             .error(dropError);

			Counter malformedMeter = registry
					.find(METER_MALFORMED)
					.counter();

			assertThat(malformedMeter).isNotNull();
			assertThat(malformedMeter.count()).isEqualTo(1);
			assertThat(errorDropped).hasValue(dropError);
		}
		finally{
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void subscribeToComplete() {
		Flux<String> source = Flux.just("foo")
		                          .delayElements(Duration.ofMillis(100))
		                          .hide();
		new FluxMetrics<>(source, registry)
				.blockLast();

		Timer stcCompleteTimer = registry.find(METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_FLOW_DURATION)
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
		new FluxMetrics<>(source, registry)
				.onErrorReturn(-1)
				.blockLast();

		Timer stcCompleteTimer = registry.find(METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_FLOW_DURATION)
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
		new FluxMetrics<>(source, registry)
				.take(1)
				.blockLast();

		Timer stcCompleteTimer = registry.find(METER_FLOW_DURATION)
		                                 .tags(Tags.of(TAG_ON_COMPLETE))
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_FLOW_DURATION)
		                              .tags(Tags.of(TAG_ON_ERROR))
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_FLOW_DURATION)
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
		Flux<Integer> test = new FluxMetrics<>(source, registry);

		test.subscribe();
		Counter meter = registry.find(METER_SUBSCRIBED)
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
		new FluxMetrics<>(source, registry)
				.blockLast();

		DistributionSummary meter = registry.find(METER_REQUESTED)
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
		new FluxMetrics<>(source, registry)
				.blockLast();

		DistributionSummary meter = registry.find(METER_REQUESTED)
		                                    .summary();

		assertThat(meter).as("global find").isNotNull();

		meter = registry.find(METER_REQUESTED)
		                .tag(TAG_SEQUENCE_NAME, "foo")
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
		new FluxMetrics<>(source, registry)
				.subscribe(bs);

		DistributionSummary meter = registry.find(METER_REQUESTED)
		                                    .tag(TAG_SEQUENCE_NAME, "foo")
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
		                            .name("normal")
		                            .hide();
		Flux<Object> source2 = Flux.error(new IllegalStateException("dummy"))
		                           .name("error")
		                           .hide();

		new FluxMetrics<>(source1, registry)
				.blockLast();
		new FluxMetrics<>(source2, registry)
				.onErrorReturn(0)
				.blockLast();

		Set<Set<String>> uniqueTagKeySets = registry
				.find(METER_FLOW_DURATION)
				.meters()
				.stream()
				.map(it -> it.getId().getTags())
				.map(it -> it.stream().map(Tag::getKey).collect(Collectors.toSet()))
				.collect(Collectors.toSet());

		assertThat(uniqueTagKeySets).hasSize(1);
	}
}
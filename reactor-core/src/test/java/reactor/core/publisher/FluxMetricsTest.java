/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;
import static reactor.util.Metrics.*;

/**
 * @author Simon Baslé
 */
public class FluxMetricsTest {

	private MeterRegistry registry;

	@Before
	public void setupRegistry() {
		registry = new SimpleMeterRegistry();
		Metrics.addRegistry(registry);
	}

	@After
	public void removeRegistry() {
		Metrics.removeRegistry(registry);
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

		assertThat(subRef.get()).isInstanceOf(FluxMetrics.MicrometerMetricsSubscriber.class);
	}

	@Test
	public void splitMetricsOnName() {
		final Flux<Integer> unnamed = Flux.<Integer>error(new IllegalStateException("boom"))
				.metrics()
				.onErrorResume(e -> Mono.empty());
		final Flux<Integer> named = Flux.range(1, 40)
		                                .name("foo")
		                                .metrics();
		Mono.when(unnamed, named).block();

		Timer unnamedMeter = registry
				.find(METER_SUBSCRIBE_TO_TERMINATE)
				.tag(TAG_SEQUENCE_NAME, REACTOR_DEFAULT_NAME)
				.timer();

		Timer namedMeter = registry
				.find(METER_SUBSCRIBE_TO_TERMINATE)
				.tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_ERROR)
				.tag(TAG_SEQUENCE_NAME, "foo")
				.timer();

		assertThat(unnamedMeter).isNotNull();
		assertThat(namedMeter).isNotNull();

		assertThat(unnamedMeter.count()).isEqualTo(1L);
		assertThat(namedMeter.count()).isZero();
	}

	@Test
	public void usesTags() {
		Flux.range(1, 8)
		    .tag("tag1", "A")
		    .name("usesTags")
		    .tag("tag2", "foo")
		    .metrics()
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
		Flux.range(1, 123)
		    .metrics()
		    .blockLast();

		Timer nextMeter = registry
				.find(METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNotNull();
		assertThat(nextMeter.count()).isEqualTo(123L);

		Flux.range(1, 10)
		    .metrics()
		    .take(3)
		    .blockLast();

		assertThat(nextMeter.count()).isEqualTo(126L);

		Flux.range(1, 1000)
		    .name("foo")
		    .metrics()
		    .blockLast();

		assertThat(nextMeter.count())
				.as("notTakingNamedIntoAccount")
				.isEqualTo(126L);
	}

	@Test
	public void malformedOnNext() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
		Flux<Integer> source = testPublisher.flux();

		source.metrics().subscribe();

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
			Flux<Integer> source = testPublisher.flux();

			source.metrics().subscribe();

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
	public void malformedOnComplete() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);
		Flux<Integer> source = testPublisher.flux();

		source.metrics().subscribe(v -> assertThat(v).isEqualTo(1),
				e -> assertThat(e).hasMessage("malformedOnComplete"));

		testPublisher.next(1)
		             .error(new IllegalStateException("malformedOnComplete"))
		             .complete();

		Counter malformedMeter = registry
				.find(METER_MALFORMED)
				.counter();

		assertThat(malformedMeter).isNotNull();
		assertThat(malformedMeter.count()).isEqualTo(1);
	}
	
	@Test
	public void subscribeToComplete() {
		Flux.just("foo")
		    .delayElements(Duration.ofMillis(100))
		    .metrics()
		    .blockLast();
		
		Timer stcCompleteTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                                 .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_COMPLETE)
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                              .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_ERROR)
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                               .tag(TAG_TERMINATION_TYPE, TAGVALUE_CANCEL)
		                               .timer();

		assertThat(stcCompleteTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to complete timer")
				.isGreaterThanOrEqualTo(100);

		assertThat(stcErrorTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to error timer")
				.isZero();

		assertThat(stcCancelTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to cancel timer")
				.isZero();
	}

	@Test
	public void subscribeToError() {
		Flux.just(1, 0)
		    .delayElements(Duration.ofMillis(100))
		    .map(v -> 100 / v)
		    .metrics()
		    .onErrorReturn(-1)
		    .blockLast();

		Timer stcCompleteTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                                 .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_COMPLETE)
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                              .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_ERROR)
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                               .tag(TAG_TERMINATION_TYPE, TAGVALUE_CANCEL)
		                               .timer();

		assertThat(stcCompleteTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to complete timer")
				.isZero();

		assertThat(stcErrorTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to error timer")
				.isGreaterThanOrEqualTo(100);

		assertThat(stcCancelTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to cancel timer")
				.isZero();
	}

	@Test
	public void subscribeToCancel() {
		Flux.just(1, 0)
		    .delayElements(Duration.ofMillis(100))
		    .metrics()
		    .take(1)
		    .blockLast();

		Timer stcCompleteTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                                 .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_COMPLETE)
		                                 .timer();

		Timer stcErrorTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                              .tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_ERROR)
		                              .timer();

		Timer stcCancelTimer = registry.find(METER_SUBSCRIBE_TO_TERMINATE)
		                               .tag(TAG_TERMINATION_TYPE, TAGVALUE_CANCEL)
		                               .timer();

		assertThat(stcCompleteTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to complete timer")
				.isZero();

		assertThat(stcErrorTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to error timer")
				.isZero();

		assertThat(stcCancelTimer.max(TimeUnit.MILLISECONDS))
				.as("subscribe to cancel timer")
				.isGreaterThanOrEqualTo(100);
	}

	@Test
	public void countsSubscriptions() {
		Flux<Integer> test = Flux.range(1, 10)
				.metrics();

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
		Flux.range(1, 10)
		    .metrics()
		    .blockLast();

		DistributionSummary meter = registry.find(METER_REQUESTED)
		                                    .summary();

		if (meter != null) { //meter could be null in some tests
			assertThat(meter.count()).isZero();
		}
	}

	@Test
	public void requestTrackingHasMeterForNamedSequence() {
		Flux.range(1, 10)
		    .name("foo")
		    .metrics()
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
		Flux.range(1, 10)
		    .name("foo")
		    .metrics()
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
}
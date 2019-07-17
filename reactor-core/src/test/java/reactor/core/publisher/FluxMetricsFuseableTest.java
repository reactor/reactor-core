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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.*;
import static reactor.core.publisher.FluxMetrics.*;

public class FluxMetricsFuseableTest {

	private MeterRegistry registry;

	@Before
	public void setupRegistry() {
		registry = new SimpleMeterRegistry();
	}

	@After
	public void removeRegistry() {
		registry.close();
	}

	// === Fuseable-specific tests ===

	@Test
	public void queueClearEmptySizeDelegates() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, Clock.SYSTEM, "foo", Tags.empty());

		Fuseable.QueueSubscription<Integer> testQueue = new FluxPeekFuseableTest.AssertQueueSubscription<>();
		testQueue.offer(1);
		assertThat(testQueue.size()).isEqualTo(1);

		fuseableSubscriber.onSubscribe(testQueue);

		assertThat(fuseableSubscriber.isEmpty()).as("isEmpty").isFalse();
		assertThat(fuseableSubscriber.size()).as("size").isEqualTo(1);

		fuseableSubscriber.clear();

		assertThat(testQueue.size()).as("original queue impacted").isZero();
		assertThat(fuseableSubscriber.size()).as("size after clear").isEqualTo(0);
	}

	@Test
	public void queueClearEmptySizeWhenQueueSubscriptionNull() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, Clock.SYSTEM, "foo", Tags.empty());

		assertThat(fuseableSubscriber.size()).as("size").isEqualTo(0);
		assertThat(fuseableSubscriber.isEmpty()).as("isEmpty").isTrue();
		assertThatCode(fuseableSubscriber::clear).doesNotThrowAnyException();
	}

	@Test
	public void queuePollTracksOnNext() {
		//prepare registry with mock clock
		MockClock clock = new MockClock();
		removeRegistry();
		registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
		Metrics.globalRegistry.add(registry);

		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, clock, "foo", Tags.empty());

		Fuseable.QueueSubscription<Integer> testQueue = new FluxPeekFuseableTest.AssertQueueSubscription<>();
		testQueue.offer(1);

		fuseableSubscriber.onSubscribe(testQueue);
		clock.add(Duration.ofMillis(200));

		Integer val1 = fuseableSubscriber.poll();
		Integer val2 = fuseableSubscriber.poll();

		assertThat(val1).isEqualTo(1);
		assertThat(val2).isNull();

		//test meters
		Timer nextTimer = registry.find(METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextTimer).isNotNull();
		assertThat(nextTimer.max(TimeUnit.MILLISECONDS)).as("onNext max delay").isEqualTo(200);
	}

	@Test
	public void queuePollSyncTracksOnComplete() {
		//prepare registry with mock clock
		MockClock clock = new MockClock();
		removeRegistry();
		registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
		Metrics.globalRegistry.add(registry);

		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, clock, "foo", Tags.empty());

		Fuseable.QueueSubscription<Integer> testQueue = new FluxPeekFuseableTest.AssertQueueSubscription<>();
		testQueue.offer(1);

		fuseableSubscriber.onSubscribe(testQueue);
		fuseableSubscriber.requestFusion(Fuseable.SYNC);

		clock.add(Duration.ofMillis(200));
		Integer val1 = fuseableSubscriber.poll();
		clock.add(Duration.ofMillis(123));
		Integer val2 = fuseableSubscriber.poll();

		assertThat(val1).isEqualTo(1);
		assertThat(val2).isNull();

		//test meters
		Timer terminationTimer = registry.find(METER_FLOW_DURATION)
		                          .tags(Tags.of(TAG_ON_COMPLETE))
		                          .timer();

		assertThat(terminationTimer).isNotNull();
		assertThat(terminationTimer.max(TimeUnit.MILLISECONDS)).as("terminate max delay").isEqualTo(323);
	}

	@Test
	public void queuePollError() {
		//prepare registry with mock clock
		MockClock clock = new MockClock();
		removeRegistry();
		registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
		Metrics.globalRegistry.add(registry);

		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, clock, "foo", Tags.empty());

		FluxPeekFuseableTest.AssertQueueSubscription<Integer> testQueue = new FluxPeekFuseableTest.AssertQueueSubscription<>();
		testQueue.setCompleteWithError(true);
		testQueue.offer(1);

		fuseableSubscriber.onSubscribe(testQueue);
		fuseableSubscriber.requestFusion(Fuseable.SYNC);

		clock.add(Duration.ofMillis(200));
		Integer val1 = fuseableSubscriber.poll();
		assertThat(val1).isEqualTo(1);

		clock.add(Duration.ofMillis(123));
		assertThatIllegalStateException().isThrownBy(fuseableSubscriber::poll)
		                                 .withMessage("AssertQueueSubscriber poll error");

		//test meters
		Timer terminationTimer = registry.find(METER_FLOW_DURATION)
		                          .tags(Tags.of(TAG_ON_ERROR))
		                          .timer();

		assertThat(terminationTimer).isNotNull();
		assertThat(terminationTimer.max(TimeUnit.MILLISECONDS)).as("terminate max delay").isEqualTo(323);
	}

	@Test
	public void requestFusionDelegates() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		FluxMetricsFuseable.MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new FluxMetricsFuseable.MetricsFuseableSubscriber<>(testSubscriber,
						registry, Clock.SYSTEM, "foo", Tags.empty());

		Fuseable.QueueSubscription<Integer> testQueue = new FluxPeekFuseableTest.AssertQueueSubscription<>();
		fuseableSubscriber.onSubscribe(testQueue);

		assertThat(fuseableSubscriber.requestFusion(Fuseable.SYNC))
				.as("fusion mode SYNC").isEqualTo(Fuseable.SYNC);

		assertThat(fuseableSubscriber.requestFusion(Fuseable.ASYNC))
				.as("fusion mode ASYNC").isEqualTo(Fuseable.ASYNC);

		assertThat(fuseableSubscriber.requestFusion(Fuseable.NONE))
				.as("fusion mode NONE").isEqualTo(Fuseable.NONE);
	}

	// === the following are Fuseable versions of FluxMetrics tests ===
	@Test
	public void testUsesMicrometerFuseable() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new FluxMetricsFuseable<>(Flux.just("foo"), registry)
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(FluxMetricsFuseable.MetricsFuseableSubscriber.class);
	}

	@Test
	public void splitMetricsOnNameFuseable() {
		final Flux<Integer> unnamedSource = Flux.just(0).map(v -> 100 / v);
		final Flux<Integer> namedSource = Flux.range(1, 40)
		                                      .map(i -> 100 / (40 - i))
		                                      .name("foo");
		
		final Flux<Integer> unnamed = new FluxMetricsFuseable<>(unnamedSource, registry)
				.onErrorResume(e -> Mono.empty());
		final Flux<Integer> named = new FluxMetricsFuseable<>(namedSource, registry)
				.onErrorResume(e -> Mono.empty());

		Mono.when(unnamed, named).block();

		Timer unnamedMeter = registry
				.find(METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_SEQUENCE_NAME, REACTOR_DEFAULT_NAME)
				.timer();

		Timer namedMeter = registry
				.find(METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_ERROR))
				.tag(TAG_SEQUENCE_NAME, "foo")
				.timer();

		assertThat(unnamedMeter).isNotNull();
		assertThat(unnamedMeter.count()).isOne();

		assertThat(namedMeter).isNotNull();
		assertThat(namedMeter.count()).isOne();
	}

	@Test
	public void usesTagsFuseable() {
		Flux<Integer> source = Flux.range(1, 8)
		                           .tag("tag1", "A")
		                           .name("usesTags")
		                           .tag("tag2", "foo");
		new FluxMetricsFuseable<>(source, registry)
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
	public void onNextTimerCountsFuseable() {
		Flux<Integer> source = Flux.range(1, 123);
		new FluxMetricsFuseable<>(source, registry)
		    .blockLast();

		Timer nextMeter = registry
				.find(METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNotNull();
		assertThat(nextMeter.count()).isEqualTo(123L);

		Flux<Integer> source2 = Flux.range(1, 10);
		new FluxMetricsFuseable<>(source2, registry)
		    .take(3)
		    .blockLast();

		assertThat(nextMeter.count()).isEqualTo(126L);

		Flux<Integer> source3 = Flux.range(1, 1000)
		    .name("foo");
		new FluxMetricsFuseable<>(source3, registry)
		    .blockLast();

		assertThat(nextMeter.count())
				.as("notTakingNamedIntoAccount")
				.isEqualTo(126L);
	}


	@Test
	public void subscribeToCompleteFuseable() {
		Flux<String> source = Flux.just(1)
		                          .doOnNext(v -> {
			                          try {
				                          Thread.sleep(100);
			                          }
			                          catch (InterruptedException e) {
				                          e.printStackTrace();
			                          }
		                          })
		                          .map(i -> "foo");
		StepVerifier.create(new FluxMetricsFuseable<>(source, registry))
		            .expectFusion(Fuseable.SYNC) //just only supports SYNC
		            .expectNext("foo")
		            .verifyComplete();

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
				.as("subscribe to error timer lazily registered")
				.isNull();

			softly.assertThat(stcCancelTimer)
				.as("subscribe to cancel timer")
				.isNull();
		});
	}

	@Test
	public void subscribeToErrorFuseable() {
		//not really fuseable, goes through onError path, but tests FluxMetricsFuseable at least
		Flux<Long> source = Flux.just(0L)
		                        .delayElements(Duration.ofMillis(100))
		                        .map(v -> 100 / v);
		new FluxMetricsFuseable<>(source, registry)
		    .onErrorReturn(-1L)
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
	public void countsSubscriptionsFuseable() {
		Flux<Integer> source = Flux.range(1, 10);
		Flux<Integer> test = new FluxMetricsFuseable<>(source, registry);

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
	public void requestTrackingDisabledIfNotNamedFuseable() {
		Flux<Integer> source = Flux.range(1, 10);
		new FluxMetricsFuseable<>(source, registry)
		    .blockLast();

		DistributionSummary meter = registry.find(METER_REQUESTED)
		                                    .summary();

		if (meter != null) { //meter could be null in some tests
			assertThat(meter.count()).isZero();
		}
	}

	@Test
	public void requestTrackingHasMeterForNamedSequenceFuseable() {
		Flux<Integer> source = Flux.range(1, 10)
		    .name("foo");
		new FluxMetricsFuseable<>(source, registry)
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
	public void requestTrackingFuseable() {
		BaseSubscriber<Integer> bs = new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				subscription.request(1);
			}
		};
		Flux<Integer> source = Flux.range(1, 10)
		    .name("foo");
		new FluxMetricsFuseable<>(source, registry)
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
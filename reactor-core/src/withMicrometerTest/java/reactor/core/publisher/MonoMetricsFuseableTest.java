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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.MonoMetricsFuseable.MetricsFuseableSubscriber;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static reactor.core.publisher.FluxMetrics.*;
import static reactor.core.publisher.FluxMetrics.TAG_ON_COMPLETE_EMPTY;

public class MonoMetricsFuseableTest {

	private MeterRegistry registry;
	private MeterRegistry previousRegistry;
	private MockClock clock;

	@BeforeEach
	public void setupRegistry() {
		clock = new MockClock();
		registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
		previousRegistry = Metrics.MicrometerConfiguration.useRegistry(registry);
	}

	@AfterEach
	public void removeRegistry() {
		registry.close();
		Metrics.MicrometerConfiguration.useRegistry(previousRegistry);
	}

	@Test
	public void scanOperator(){
		MonoMetricsFuseable<String> test = new MonoMetricsFuseable<>(Mono.just("foo"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber(){
		CoreSubscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MetricsFuseableSubscriber<Integer> test = new MetricsFuseableSubscriber<>(actual, registry, Clock.SYSTEM, "foo", Tags.empty());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	// === Fuseable-specific tests ===

	@Test
	public void queueClearEmptySizeDelegates() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
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
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
						registry, Clock.SYSTEM, "foo", Tags.empty());

		assertThat(fuseableSubscriber.size()).as("size").isEqualTo(0);
		assertThat(fuseableSubscriber.isEmpty()).as("isEmpty").isTrue();
		assertThatCode(fuseableSubscriber::clear).doesNotThrowAnyException();
	}

	@Test
	public void queuePollDoesntTrackOnNext() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
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
		Timer nextTimer = registry.find("foo" + METER_ON_NEXT_DELAY).timer();

		assertThat(nextTimer).as("no onNext delay meter for Mono").isNull();
	}

	@Test
	public void queuePollSyncTracksOnComplete() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
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
		Timer terminationTimer = registry.find("foo" + METER_FLOW_DURATION)
		                          .tags(Tags.of(TAG_ON_COMPLETE))
		                          .timer();

		assertThat(terminationTimer).isNotNull();
		assertThat(terminationTimer.max(TimeUnit.MILLISECONDS)).as("terminate max delay").isEqualTo(200);
	}

	@Test
	public void queuePollError() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
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
		Timer terminationTimer = registry.find("foo" + METER_FLOW_DURATION)
		                          .tags(Tags.of(TAG_ON_ERROR))
		                          .timer();

		assertThat(terminationTimer).isNotNull();
		assertThat(terminationTimer.max(TimeUnit.MILLISECONDS)).as("terminate max delay").isEqualTo(323);
	}

	@Test
	public void requestFusionDelegates() {
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();
		MetricsFuseableSubscriber<Integer> fuseableSubscriber =
				new MetricsFuseableSubscriber<>(testSubscriber,
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

	// === the following are Fuseable versions of MonoMetrics tests ===
	@Test
	public void testUsesMicrometerFuseable() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new MonoMetricsFuseable<>(Mono.just("foo"))
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(MetricsFuseableSubscriber.class);
	}

	@Test
	public void splitMetricsOnNameFuseable() {
		final Mono<Integer> unnamedSource = Mono.just(0).map(v -> 100 / v);
		final Mono<Integer> namedSource = Mono.just(0).map(v -> 100 / v).name("foo");
		
		final Mono<Integer> unnamed = new MonoMetricsFuseable<>(unnamedSource)
				.onErrorResume(e -> Mono.empty());
		final Mono<Integer> named = new MonoMetricsFuseable<>(namedSource)
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
		assertThat(unnamedMeter.count()).isOne();

		assertThat(namedMeter).isNotNull();
		assertThat(namedMeter.count()).isOne();
	}

	@Test
	public void usesTagsFuseable() {
		Mono<Integer> source = Mono.just(8)
								   .name("usesTags")
								   .tag("tag1", "A")
		                           .tag("tag2", "foo");

		new MonoMetricsFuseable<>(source).block();

		Timer meter = registry
				.find("usesTags" + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.tag("tag1", "A")
				.tag("tag2", "foo")
				.timer();

		assertThat(meter).isNotNull();
		assertThat(meter.count()).isEqualTo(1L);
	}

	@Test
	public void noOnNextTimerFuseable() {
		Mono<Integer> source = Mono.just(123);
		new MonoMetricsFuseable<>(source)
		    .block();

		Timer nextMeter = registry
				.find(REACTOR_DEFAULT_NAME + METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNull();
	}

	@Test
	public void completeEmptyNoFusion() {
		Mono<Integer> source = Mono.<Integer>empty().hide();

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectNoFusionSupport()
		            .verifyComplete();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNull();

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());
	}

	@Test
	public void completeEmptyAsyncFusion() {
		Mono<Integer> source = Mono.fromCallable(() -> null);

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectFusion(Fuseable.ASYNC)
		            .verifyComplete();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNull();

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());
	}

	@Test
	public void completeEmptySyncFusion() {
		MonoMetricsFuseable.MetricsFuseableSubscriber<Object> subscriber =
				new MetricsFuseableSubscriber<>(AssertSubscriber.create(),
						registry, clock, REACTOR_DEFAULT_NAME, DEFAULT_TAGS_MONO);

		//trigger the fusion and polling
		subscriber.onSubscribe(new FluxPeekFuseableTest.AssertQueueSubscription<>());
		assertThat(subscriber.requestFusion(Fuseable.SYNC)).as("SYNC requested").isEqualTo(Fuseable.SYNC);
		assertThat(subscriber.poll()).as("poll empty").isNull();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNull();

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());
	}

	@Test
	public void completeWithElementNoFusion() {
		Mono<Integer> source = Mono.just(1).hide();

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .verifyComplete();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                                   .tags(Tags.of(TAG_ON_COMPLETE))
		                                   .timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
		                                        .tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
		                                        .timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNull();
	}

	@Test
	public void completeWithElementAsyncFusion() {
		Mono<Integer> source = Mono.fromCallable(() -> 1);

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(1)
		            .verifyComplete();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNull();
	}

	@Test
	public void completeWithElementSyncFusion() {
		Mono<Integer> source = Mono.just(1);

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext(1)
		            .verifyComplete();

		Timer stcCompleteCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE))
				.timer();

		Timer stcCompleteEmptyCounter = registry.find(REACTOR_DEFAULT_NAME + METER_FLOW_DURATION)
				.tags(Tags.of(TAG_ON_COMPLETE_EMPTY))
				.timer();

		assertThat(stcCompleteCounter)
				.as("complete with element")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).as("timer count").isOne());

		assertThat(stcCompleteEmptyCounter)
				.as("complete without any element")
				.isNull();
	}

	@Test
	public void subscribeToCompleteFuseable() {
		Mono<String> source = Mono.fromCallable(() -> {
			Thread.sleep(100);
			return "foo";
		});

		StepVerifier.create(new MonoMetricsFuseable<>(source))
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext("foo")
		            .verifyComplete();


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
			      .as("subscribe to error timer lazily registered")
			      .isNull();

			softly.assertThat(stcCancelTimer)
			      .as("subscribe to cancel timer")
			      .isNull();
		});
	}

	@Test
	public void subscribeToErrorFuseable() {
		Mono<Long> source = Mono.delay(Duration.ofMillis(100))
		                        .map(v -> 100 / v);
		new MonoMetricsFuseable<>(source)
		    .onErrorReturn(-1L)
		    .block();

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
	public void subscribeToCancelFuseable() throws InterruptedException {
		Mono<String> source = Mono.delay(Duration.ofMillis(200))
		                        .map(i -> "foo");
		Disposable disposable = new MonoMetricsFuseable<>(source).subscribe();
		Thread.sleep(100);
		disposable.dispose();

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
	public void countsSubscriptionsFuseable() {
		Mono<Integer> source = Mono.just(10);
		Mono<Integer> test = new MonoMetricsFuseable<>(source);

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
	public void noRequestTrackingEvenForNamedSequence() {
		Mono<Integer> source = Mono.just(10)
		                           .name("foo");
		new MonoMetricsFuseable<>(source)
				.block();

		DistributionSummary meter = registry.find("foo" + METER_REQUESTED)
		                                    .summary();

		assertThat(meter).as("global find").isNull();

		meter = registry.find(METER_REQUESTED)
		                .name("foo")
		                .summary();

		assertThat(meter).as("tagged find").isNull();
	}
}

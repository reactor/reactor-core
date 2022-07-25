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

package reactor.core.observability.micrometer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Simon Baslé
 */
class MicrometerMeterListenerTest {

	SimpleMeterRegistry registry;
	AtomicLong virtualClockTime;
	Clock                                virtualClock;
	MicrometerMeterListenerConfiguration configuration;

	@BeforeEach
	void initRegistry() {
		registry = new SimpleMeterRegistry();
		virtualClockTime = new AtomicLong();
		virtualClock = new Clock() {
			@Override
			public long wallTime() {
				return virtualClockTime.get();
			}

			@Override
			public long monotonicTime() {
				return virtualClockTime.get();
			}
		};
		configuration = new MicrometerMeterListenerConfiguration(
			"testName",
			Tags.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			virtualClock,
			false);
	}

	@Test
	void initialStateFluxWithDefaultName() {
		configuration = new MicrometerMeterListenerConfiguration(
			Micrometer.DEFAULT_METER_PREFIX,
			Tags.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			virtualClock,
			false);

		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.requestedCounter).as("requestedCounter disabled").isNull();
		assertThat(listener.onNextIntervalTimer).as("onNextIntervalTimer").isNotNull();

		assertThat(registry.getMetersAsString().split("\n"))
			.as("registered meters: onNextIntervalTimer")
			.containsExactly(
				Micrometer.DEFAULT_METER_PREFIX + ".onNext.delay(TIMER)[testTag1='testTagValue1', testTag2='testTagValue2']; count=0.0, total_time=0.0 seconds, max=0.0 seconds"
			);

		assertThat(registry.remove(listener.onNextIntervalTimer))
			.as("registry contains onNextIntervalTimer")
			.isSameAs(listener.onNextIntervalTimer);
	}

	@Test
	void initialStateFluxWithCustomName() {
		configuration = new MicrometerMeterListenerConfiguration(
			"testName",
			Tags.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			virtualClock,
			false);

		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.requestedCounter).as("requestedCounter").isNotNull();
		assertThat(listener.onNextIntervalTimer).as("onNextIntervalTimer").isNotNull();

		assertThat(registry.getMetersAsString().split("\n"))
			.as("registered meters: onNextIntervalTimer, requestedCounter")
			.containsExactly(
				"testName.onNext.delay(TIMER)[testTag1='testTagValue1', testTag2='testTagValue2']; count=0.0, total_time=0.0 seconds, max=0.0 seconds",
				"testName.requested(DISTRIBUTION_SUMMARY)[testTag1='testTagValue1', testTag2='testTagValue2']; count=0.0, total=0.0, max=0.0"
			);

		assertThat(registry.remove(listener.onNextIntervalTimer))
			.as("registry contains onNextIntervalTimer")
			.isSameAs(listener.onNextIntervalTimer);

		assertThat(registry.remove(listener.requestedCounter))
			.as("registry contains requestedCounter")
			.isSameAs(listener.requestedCounter);
	}

	@Test
	void initialStateMono() {
		configuration = new MicrometerMeterListenerConfiguration(
			Micrometer.DEFAULT_METER_PREFIX,
			Tags.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			virtualClock,
			true);

		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.requestedCounter).as("requestedCounter disabled").isNull();
		assertThat(listener.onNextIntervalTimer).as("onNextIntervalTimer disabled").isNull();

		assertThat(registry.getMetersAsString())
			.as("no registered meters")
			.isEmpty();
	}

	@Test
	void timerSampleInitializedInSubscription() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		assertThat(listener.subscribeToTerminateSample)
			.as("subscribeToTerminateSample pre subscription")
			.isNull();
		assertThat(listener.lastNextEventNanos)
			.as("lastNextEventNanos")
			.isEqualTo(-1L);
		assertThat(registry.getMeters())
			.as("meters pre subscription")
			.hasSize(2);

		virtualClockTime.incrementAndGet();
		listener.doOnSubscription();

		assertThat(listener.subscribeToTerminateSample)
			.as("subscribeToTerminateSample")
			.isNotNull();
		assertThat(listener.lastNextEventNanos)
			.as("lastNextEventNanos")
			.isEqualTo(1L);
		assertThat(registry.getMeters())
			.as("meters post subscription")
			.hasSize(3);
		assertThat(registry.find("testName" + MicrometerMeterListener.METER_SUBSCRIBED).counter())
			.as("meter .subscribed")
			.isNotNull()
			.satisfies(meter -> assertThat(meter.count()).isEqualTo(1d));
	}

	@Test
	void doOnCancelTimesFlowDurationMeter() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();
		assertThat(registry.getMeters()).hasSize(3);

		virtualClockTime.set(100);
		listener.doOnCancel();

		Timer timer = registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION)
			.timer();

		assertThat(registry.getMeters()).hasSize(4);
		assertThat(timer.getId().toString())
			.as(".flow.duration with exception and status tags")
			.isEqualTo("MeterId{name='testName.flow.duration', tags=[tag(exception=),tag(status=cancelled),tag(testTag1=testTagValue1),tag(testTag2=testTagValue2)]}");
		assertThat(timer.totalTime(TimeUnit.NANOSECONDS))
			.as("measured time")
			.isEqualTo(100);
	}

	@Test
	void doOnCompleteTimesFlowDurationMeter_completeEmpty() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();
		assertThat(registry.getMeters()).hasSize(3);

		virtualClockTime.set(100);
		listener.doOnComplete();

		Timer timer = registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION)
			.timer();

		assertThat(registry.getMeters()).hasSize(4);
		assertThat(timer.getId().toString())
			.as(".flow.duration with exception and status tags")
			.isEqualTo("MeterId{name='testName.flow.duration', tags=[tag(exception=),tag(status=completedEmpty),tag(testTag1=testTagValue1),tag(testTag2=testTagValue2)]}");
		assertThat(timer.totalTime(TimeUnit.NANOSECONDS))
			.as("measured time")
			.isEqualTo(100);
	}

	@Test
	void doOnCompleteTimesFlowDurationMeter_completeValued() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();
		assertThat(registry.getMeters()).hasSize(3);

		virtualClockTime.set(100);
		listener.valued = true;
		listener.doOnComplete();

		Timer timer = registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION)
			.timer();

		assertThat(registry.getMeters()).hasSize(4);
		assertThat(timer.getId().toString())
			.as(".flow.duration with exception and status tags")
			.isEqualTo("MeterId{name='testName.flow.duration', tags=[tag(exception=),tag(status=completed),tag(testTag1=testTagValue1),tag(testTag2=testTagValue2)]}");
		assertThat(timer.totalTime(TimeUnit.NANOSECONDS))
			.as("measured time")
			.isEqualTo(100);
	}

	@Test
	void doOnErrorTimesFlowDurationMeter() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();
		assertThat(registry.getMeters()).hasSize(3);

		virtualClockTime.set(100);
		listener.doOnError(new IllegalStateException("expected"));

		Timer timer = registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION)
			.timer();

		assertThat(registry.getMeters()).hasSize(4);
		assertThat(timer.getId().toString())
			.as(".flow.duration with exception and status tags")
			.isEqualTo("MeterId{name='testName.flow.duration', tags=[tag(exception=java.lang.IllegalStateException),tag(status=error),tag(testTag1=testTagValue1),tag(testTag2=testTagValue2)]}");
		assertThat(timer.totalTime(TimeUnit.NANOSECONDS))
			.as("measured time")
			.isEqualTo(100);
	}

	@Test
	void doOnNextRecordsInterval() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();

		virtualClockTime.set(100);
		listener.doOnNext(1);

		assertThat(listener.valued).as("valued").isTrue();
		assertThat(listener.lastNextEventNanos).as("lastEventNanos recorded").isEqualTo(100);

		assertThat(registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION).meters())
			.as("no flow.duration meter yet")
			.isEmpty();

		assertThat(listener.onNextIntervalTimer.totalTime(TimeUnit.NANOSECONDS))
			.as("interval timed")
			.isEqualTo(100);
		assertThat(listener.onNextIntervalTimer.count())
			.as("onNext count")
			.isOne();
	}


	@Test
	void doOnNextRecordsInterval_defaultName() {
		configuration = new MicrometerMeterListenerConfiguration(Micrometer.DEFAULT_METER_PREFIX, Tags.empty(),
			registry, virtualClock, false);
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();

		virtualClockTime.set(100);
		listener.doOnNext(1);

		assertThat(listener.valued).as("valued").isTrue();
		assertThat(listener.lastNextEventNanos).as("lastEventNanos recorded").isEqualTo(100);

		assertThat(registry.find(Micrometer.DEFAULT_METER_PREFIX + MicrometerMeterListener.METER_FLOW_DURATION).meters())
			.as("no flow.duration meter yet")
			.isEmpty();

		assertThat(listener.onNextIntervalTimer.totalTime(TimeUnit.NANOSECONDS))
			.as("interval timed")
			.isEqualTo(100);
		assertThat(listener.onNextIntervalTimer.count())
			.as("onNext count")
			.isOne();
	}

	@Test
	void doOnNext_monoRecordsCompletionOnly() {
		configuration = new MicrometerMeterListenerConfiguration("testName", Tags.empty(),
			registry, virtualClock, true);
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		listener.doOnSubscription();

		virtualClockTime.set(100);
		listener.doOnNext(1);

		assertThat(listener.valued).as("valued").isTrue();
		assertThat(listener.lastNextEventNanos).as("no lastEventNanos recorded").isZero();

		Timer timer = registry.find("testName" + MicrometerMeterListener.METER_FLOW_DURATION)
			.timer();

		assertThat(timer.getId().toString())
			.as(".flow.duration with exception and status tags")
			.isEqualTo("MeterId{name='testName.flow.duration', tags=[tag(exception=),tag(status=completed)]}");
		assertThat(timer.totalTime(TimeUnit.NANOSECONDS))
			.as("measured time")
			.isEqualTo(100);
	}

	@Test
	void doOnNextMultipleRecords() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnSubscription();

		virtualClockTime.set(100);
		listener.doOnNext(1);
		virtualClockTime.set(300);
		listener.doOnNext(2);

		assertThat(listener.onNextIntervalTimer.totalTime(TimeUnit.NANOSECONDS))
			.as("total time")
			.isEqualTo(300);
		assertThat(listener.onNextIntervalTimer.count())
			.as("onNext count")
			.isEqualTo(2);
		assertThat(listener.onNextIntervalTimer.max(TimeUnit.NANOSECONDS))
			.as("onNext max interval")
			.isEqualTo(200);
	}

	@Test
	void doOnRequestRecordsTotalDemand() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		listener.doOnRequest(100L);

		assertThat(listener.requestedCounter.count()).as("1 request calls").isEqualTo(1);
		assertThat(listener.requestedCounter.totalAmount()).as("total after first request").isEqualTo(100);

		listener.doOnRequest(200L);

		assertThat(listener.requestedCounter.count()).as("2 request calls").isEqualTo(2);
		assertThat(listener.requestedCounter.totalAmount()).as("total after second request").isEqualTo(300);
		assertThat(listener.requestedCounter.max()).as("max is second request").isEqualTo(200);
	}

	@Test
	void doOnRequestMonoIgnoresRequest() {
		configuration = new MicrometerMeterListenerConfiguration("testName", Tags.empty(), registry, virtualClock, true);
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		assertThatCode(() -> listener.doOnRequest(100L)).doesNotThrowAnyException();
		assertThat(listener.requestedCounter).isNull();
	}

	@Test
	void doOnRequestDefaultNameIgnoresRequest() {
		configuration = new MicrometerMeterListenerConfiguration(Micrometer.DEFAULT_METER_PREFIX, Tags.empty(), registry, virtualClock, false);
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);
		assertThatCode(() -> listener.doOnRequest(100L)).doesNotThrowAnyException();
		assertThat(listener.requestedCounter).isNull();
	}

	@Test
	void malformedCounterCapturesNextCompleteError() {
		MicrometerMeterListener<Integer> listener = new MicrometerMeterListener<>(configuration);

		Counter malformedCounter = registry.find("testName" + MicrometerMeterListener.METER_MALFORMED).counter();
		assertThat(malformedCounter).as("counter not registered").isNull();

		listener.doOnMalformedOnNext(123);

		malformedCounter = registry.find("testName" + MicrometerMeterListener.METER_MALFORMED).counter();
		assertThat(malformedCounter).as("lazy counter registration").isNotNull();
		assertThat(malformedCounter.count()).as("onNext malformed").isOne();

		listener.doOnMalformedOnComplete();
		assertThat(malformedCounter.count()).as("onComplete malformed").isEqualTo(2);

		listener.doOnMalformedOnError(new IllegalStateException("expected, ignored"));
		assertThat(malformedCounter.count()).as("onError malformed").isEqualTo(3);
	}
}
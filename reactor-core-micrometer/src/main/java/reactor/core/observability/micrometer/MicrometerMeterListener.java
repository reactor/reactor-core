/*
 * Copyright (c) 2022-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.jspecify.annotations.Nullable;
import reactor.core.observability.SignalListener;
import reactor.core.publisher.SignalType;

import static reactor.core.observability.micrometer.MicrometerMeterListenerDocumentation.CommonTags.*;
import static reactor.core.observability.micrometer.MicrometerMeterListenerDocumentation.TerminationTags.*;

/**
 * A {@link SignalListener} that activates metrics gathering using Micrometer 1.x.
 *
 * @author Simon Baslé
 */
final class MicrometerMeterListener<T> implements SignalListener<T> {

	final           MicrometerMeterListenerConfiguration configuration;
	final @Nullable DistributionSummary                  requestedCounter;
	final @Nullable Timer                                onNextIntervalTimer;

	Timer.Sample subscribeToTerminateSample;
	long         lastNextEventNanos = -1L;
	boolean      valued;

	MicrometerMeterListener(MicrometerMeterListenerConfiguration configuration) {
		this.configuration = configuration;

		this.valued = false;
		if (configuration.isMono) {
			//for Mono we don't record onNextInterval (since there is at most 1 onNext).
			//Note that we still need a mean to distinguish between empty Mono and valued Mono for recordOnCompleteEmpty
			onNextIntervalTimer = null;
			//we also don't count the number of request calls (there should be only one)
			requestedCounter = null;
		}
		else {
			this.onNextIntervalTimer = Timer.builder(MicrometerMeterListenerDocumentation.ON_NEXT_DELAY.getName(configuration.sequenceName))
				.tags(configuration.commonTags)
				.register(configuration.registry);

			if (!Micrometer.DEFAULT_METER_PREFIX.equals(configuration.sequenceName)) {
				this.requestedCounter = DistributionSummary.builder(MicrometerMeterListenerDocumentation.REQUESTED_AMOUNT.getName(configuration.sequenceName))
					.tags(configuration.commonTags)
					.register(configuration.registry);
			}
			else {
				requestedCounter = null;
			}
		}
	}

	@Override
	public void doOnCancel() {
		//we don't record the time between last onNext and cancel,
		// because it would skew the onNext count by one
		recordCancel(configuration.sequenceName, configuration.commonTags, configuration.registry, subscribeToTerminateSample);
	}

	@Override
	public void doOnComplete() {
		//we don't record the time between last onNext and onComplete,
		// because it would skew the onNext count by one.
		// We differentiate between empty completion and value completion, however, via tags.
		if (!valued) {
			recordOnCompleteEmpty(configuration.sequenceName, configuration.commonTags, configuration.registry, subscribeToTerminateSample);
		}
		else if (!configuration.isMono) {
			//recordOnComplete is done directly in onNext for the Mono(valued) case
			recordOnComplete(configuration.sequenceName, configuration.commonTags, configuration.registry, subscribeToTerminateSample);
		}
	}

	@Override
	public void doOnMalformedOnComplete() {
		recordMalformed(configuration.sequenceName, configuration.commonTags, configuration.registry);
	}

	@Override
	public void doOnError(Throwable e) {
		//we don't record the time between last onNext and onError,
		// because it would skew the onNext count by one
		recordOnError(configuration.sequenceName, configuration.commonTags, configuration.registry, subscribeToTerminateSample, e);
	}

	@Override
	public void doOnMalformedOnError(Throwable e) {
		recordMalformed(configuration.sequenceName, configuration.commonTags, configuration.registry);
	}

	@Override
	public void doOnNext(T t) {
		valued = true;
		if (onNextIntervalTimer == null) { //NB: interval timer is only null if isMono
			//record valued completion directly
			recordOnComplete(configuration.sequenceName, configuration.commonTags, configuration.registry, subscribeToTerminateSample);
			return;
		}
		//record the delay since previous onNext/onSubscribe. This also records the count.
		long last = this.lastNextEventNanos;
		this.lastNextEventNanos = configuration.registry.config().clock().monotonicTime();
		this.onNextIntervalTimer.record(lastNextEventNanos - last, TimeUnit.NANOSECONDS);
	}

	@Override
	public void doOnMalformedOnNext(T value) {
		recordMalformed(configuration.sequenceName, configuration.commonTags, configuration.registry);
	}

	@Override
	public void doOnSubscription() {
		recordOnSubscribe(configuration.sequenceName, configuration.commonTags, configuration.registry);
		this.subscribeToTerminateSample = Timer.start(configuration.registry);
		this.lastNextEventNanos = configuration.registry.config().clock().monotonicTime();
	}

	@Override
	public void doOnRequest(long l) {
		if (requestedCounter != null) {
			requestedCounter.record(l);
		}
	}

	//unused hooks

	@Override
	public void doFirst() {
		// NO-OP
	}

	@Override
	public void doOnFusion(int negotiatedFusion) throws Throwable {
		// NO-OP
		//TODO metrics counting fused (with ASYNC/SYNC tags) could be implemented to supplement METER_SUBSCRIBED
	}

	@Override
	public void doFinally(SignalType terminationType) {
		// NO-OP
	}

	@Override
	public void doAfterComplete() {
		// NO-OP
	}

	@Override
	public void doAfterError(Throwable error) {
		// NO-OP
	}

	@Override
	public void handleListenerError(Throwable listenerError) {
		// NO-OP
	}


	// == from KeyNames to Tags
	static final Tags  DEFAULT_TAGS_FLUX = Tags.of(TYPE.asString(), TAG_TYPE_FLUX);
	static final Tags  DEFAULT_TAGS_MONO = Tags.of(TYPE.asString(), TAG_TYPE_MONO);
	static final Tag  TAG_ON_ERROR    = Tag.of(STATUS.asString(), TAG_STATUS_ERROR);
	static final Tags TAG_ON_COMPLETE = Tags.of(STATUS.asString(), TAG_STATUS_COMPLETED, EXCEPTION.asString(), "");
	static final Tags TAG_ON_COMPLETE_EMPTY = Tags.of(STATUS.asString(), TAG_STATUS_COMPLETED_EMPTY, EXCEPTION.asString(), "");
	static final Tags TAG_CANCEL            = Tags.of(STATUS.asString(), TAG_STATUS_CANCELLED, EXCEPTION.asString(), "");

	// === Record methods ===

	/*
	 * This method calls the registry, which can be costly. However the cancel signal is only expected
	 * once per Subscriber. So the net effect should be that the registry is only called once, which
	 * is equivalent to registering the meter as a final field, with the added benefit of paying that
	 * cost only in case of cancellation.
	 */
	static void recordCancel(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(MicrometerMeterListenerDocumentation.FLOW_DURATION.getName(name))
			.tags(commonTags.and(TAG_CANCEL))
			.description(
				"Times the duration elapsed between a subscription and the cancellation of the sequence")
			.register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However a malformed signal is generally
	 * not expected, or at most once per Subscriber. So the net effect should be that the registry
	 * is only called once, which is equivalent to registering the meter as a final field,
	 * with the added benefit of paying that cost only in case of onNext/onError after termination.
	 */
	static void recordMalformed(String name, Tags commonTags, MeterRegistry registry) {
		registry.counter(MicrometerMeterListenerDocumentation.MALFORMED_SOURCE_EVENTS.getName(name), commonTags)
			.increment();
	}

	/*
	 * This method calls the registry, which can be costly. However the onError signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of error.
	 */
	static void recordOnError(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration,
							  Throwable e) {
		Timer timer = Timer.builder(MicrometerMeterListenerDocumentation.FLOW_DURATION.getName(name))
			.tags(commonTags.and(TAG_ON_ERROR))
			.tag(MicrometerMeterListenerDocumentation.TerminationTags.EXCEPTION.asString(),
				e.getClass().getName())
			.description(
				"Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag.")
			.register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onComplete signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of completion (which is not always occurring).
	 */
	static void recordOnComplete(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(MicrometerMeterListenerDocumentation.FLOW_DURATION.getName(name))
			.tags(commonTags.and(TAG_ON_COMPLETE))
			.description(
				"Times the duration elapsed between a subscription and the onComplete termination of a sequence that did emit some elements")
			.register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onComplete signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of completion (which is not always occurring).
	 */
	static void recordOnCompleteEmpty(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(MicrometerMeterListenerDocumentation.FLOW_DURATION.getName(name))
			.tags(commonTags.and(TAG_ON_COMPLETE_EMPTY))
			.description(
				"Times the duration elapsed between a subscription and the onComplete termination of a sequence that didn't emit any element")
			.register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onSubscribe signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of subscription.
	 */
	static void recordOnSubscribe(String name, Tags commonTags, MeterRegistry registry) {
		Counter.builder(MicrometerMeterListenerDocumentation.SUBSCRIBED.getName(name))
			.tags(commonTags)
			.register(registry)
			.increment();
	}

}

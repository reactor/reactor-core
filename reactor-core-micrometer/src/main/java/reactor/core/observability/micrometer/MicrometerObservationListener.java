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

import io.micrometer.context.ContextSnapshot;
import io.micrometer.observation.Observation;

import reactor.core.observability.SignalListener;
import reactor.core.publisher.SignalType;
import reactor.util.context.ContextView;

/**
 * A {@link SignalListener} that makes timings using the {@link io.micrometer.observation.Observation} API from Micrometer 1.10.
 * <p>
 * This is a bare-bone version compared to {@link MicrometerMeterListener}, as it only opens a single Observation for the
 * duration of the Flux/Mono.
 *
 * @author Simon Baslé
 */
final class MicrometerObservationListener<T> implements SignalListener<T> {

	static final String OBSERVATION_FLOW = ".observation.flow";

	/**
	 * A value for the status tag, to be used when a Mono completes from onNext.
	 * In production, this is set to {@link MicrometerMeterListener#TAG_STATUS_COMPLETED}.
	 * In some tests, this can be overridden as a way to assert {@link #doOnComplete()} is no-op.
	 */
	final String                                     completedOnNextStatus;
	final MicrometerObservationListenerConfiguration configuration;
	final ContextView                                context;
	final Observation                                subscribeToTerminalObservation;

	boolean valued;

	MicrometerObservationListener(ContextView subscriberContext, MicrometerObservationListenerConfiguration configuration) {
		this(subscriberContext, configuration, MicrometerMeterListener.TAG_STATUS_COMPLETED);
	}

	//for test purposes, we can pass in a value for the status tag, to be used when a Mono completes from onNext
	MicrometerObservationListener(ContextView subscriberContext, MicrometerObservationListenerConfiguration configuration, String completedOnNextStatus) {
		this.configuration = configuration;
		this.context = subscriberContext;
		this.completedOnNextStatus = completedOnNextStatus;

		this.valued = false;

		subscribeToTerminalObservation = Observation.createNotStarted(
			configuration.sequenceName + OBSERVATION_FLOW,
			configuration.registry
		).lowCardinalityKeyValues(configuration.commonKeyValues);
	}

	@Override
	public void doOnCancel() {
		Observation observation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, MicrometerMeterListener.TAG_STATUS_CANCELLED);

		observation.stop();
	}

	@Override
	public void doOnComplete() {
		// We differentiate between empty completion and value completion via tags.
		String status = null;
		if (!valued) {
			status = MicrometerMeterListener.TAG_STATUS_COMPLETED_EMPTY;
		}
		else if (!configuration.isMono) {
			status = MicrometerMeterListener.TAG_STATUS_COMPLETED;
		}

		// if status == null, recording with OnComplete tag is done directly in onNext for the Mono(valued) case
		if (status != null) {
			Observation completeObservation = subscribeToTerminalObservation
				.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, status);

			completeObservation.stop();
		}
	}

	@Override
	public void doOnError(Throwable e) {
		Observation errorObservation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, MicrometerMeterListener.TAG_STATUS_ERROR)
			.error(e);

		errorObservation.stop();
	}

	@Override
	public void doOnNext(T t) {
		valued = true;
		if (configuration.isMono) {
			//record valued completion directly
			Observation completeObservation = subscribeToTerminalObservation
				.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, completedOnNextStatus);

			completeObservation.stop();
		}
	}

	@Override
	public void doOnSubscription() {
		ContextSnapshot contextSnapshot = ContextSnapshot.forContextAndThreadLocalValues(this.context);
		try (ContextSnapshot.Scope ignored = contextSnapshot.setThreadLocalValues()) {
			this.subscribeToTerminalObservation.start();
		}
	}

	//unused hooks

	@Override
	public void doOnMalformedOnComplete() {
		//NO-OP
	}

	@Override
	public void doOnMalformedOnError(Throwable e) {
		// NO-OP
	}

	@Override
	public void doOnMalformedOnNext(T value) {
		// NO-OP
	}

	@Override
	public void doOnRequest(long l) {
		// NO-OP
	}

	@Override
	public void doFirst() {
		// NO-OP
	}

	@Override
	public void doOnFusion(int negotiatedFusion) {
		// NO-OP
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

}

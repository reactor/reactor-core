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

import io.micrometer.observation.Observation;

import reactor.core.observability.SignalListener;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

/**
 * A {@link SignalListener} that makes timings using the {@link io.micrometer.observation.Observation} API from Micrometer 1.10.
 * <p>
 * This is a transposition of {@link MicrometerMeterListener}, but only retains the timers.
 *
 * @author Simon Baslé
 */
final class MicrometerObservationListener<T> implements SignalListener<T> {

	static final String OBSERVATION_FLOW = ".observation.flow";
	static final String OBSERVATION_VALUES = ".observation.values";


	final MicrometerObservationListenerConfiguration configuration;
	@Nullable
	final Observation                                signalIntervalObservation;
	final Observation                                subscribeToTerminalObservation;

	/**
	 * Scope is first opened in onSubscribe.
	 * It is then closed and opened in each onNext.
	 * Finally, last Scope is closed in either of the terminal states (cancelled, completed, error).
	 */
	@Nullable
	Observation.Scope onNextScope;

	boolean valued;

	MicrometerObservationListener(MicrometerObservationListenerConfiguration configuration) {
		this.configuration = configuration;

		this.valued = false;
		if (configuration.isMono) {
			//for Mono we don't record signalInterval (since there is at most 1 onNext, it's the same as subscribeToTerminalObservation).
			//Note that we still need a mean to distinguish between empty Mono and valued Mono for recordOnCompleteEmpty
			signalIntervalObservation = null;
			onNextScope = Observation.Scope.NOOP;
		}
		else {
			this.signalIntervalObservation = Observation.createNotStarted(
				configuration.sequenceName + OBSERVATION_VALUES,
				configuration.registry
			).lowCardinalityKeyValues(configuration.commonKeyValues);
		}

		subscribeToTerminalObservation = Observation.createNotStarted(
			configuration.sequenceName + OBSERVATION_FLOW,
			configuration.registry
		).lowCardinalityKeyValues(configuration.commonKeyValues);
	}

	@Override
	public void doOnCancel() {
		//Due to dealing with scopes, we have to stop the current scope.
		//Note this could skew the onNext counter off by one if the Observation is a facade over metrics.
		if (this.onNextScope != null) {
			this.onNextScope.close();
		}

		Observation observation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, MicrometerMeterListener.TAG_STATUS_CANCELLED)
			.highCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_EXCEPTION, "");

		observation.stop();
	}

	@Override
	public void doOnComplete() {
		//Due to dealing with scopes, we have to stop the current scope.
		//Note this could skew the onNext counter off by one if the Observation is a facade over metrics.
		if (this.onNextScope != null) {
			this.onNextScope.close();
		}

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
				.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, status)
				.highCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_EXCEPTION, "");

			completeObservation.stop();
		}
	}

	@Override
	public void doOnError(Throwable e) {
		//Due to dealing with scopes, we have to stop the current scope.
		//Note this could skew the onNext counter off by one if the Observation is a facade over metrics.
		if (this.onNextScope != null) {
			this.onNextScope.close();
		}

		Observation errorObservation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, MicrometerMeterListener.TAG_STATUS_ERROR)
			.highCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_EXCEPTION, e.getClass().getName());

		errorObservation.stop();
	}

	@Override
	public void doOnNext(T t) {
		valued = true;
		if (signalIntervalObservation == null || onNextScope == null) { //NB: interval observation is only null if isMono
			//record valued completion directly
			Observation completeObservation = subscribeToTerminalObservation
				.lowCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_STATUS, MicrometerMeterListener.TAG_STATUS_COMPLETED)
				.highCardinalityKeyValue(MicrometerMeterListener.TAG_KEY_EXCEPTION, "");

			completeObservation.stop();
			return;
		}
		//record the delay since previous onNext/onSubscribe. This also records the count.
		this.onNextScope.close();
		this.onNextScope = this.signalIntervalObservation.openScope();
	}

	@Override
	public void doOnSubscription() {
		this.subscribeToTerminalObservation.start();
		if (this.signalIntervalObservation != null) {
			this.signalIntervalObservation.start();
			this.onNextScope = this.signalIntervalObservation.openScope();
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

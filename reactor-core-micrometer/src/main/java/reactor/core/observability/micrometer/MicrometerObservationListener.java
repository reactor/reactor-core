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
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
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

	private static final Logger LOGGER = Loggers.getLogger(MicrometerObservationListener.class);

	static final String OBSERVATION_SUFFIX_FOR_METRICS = ".observation";
	static final String KEY_STATUS = "reactor.status";
	static final String KEY_TYPE = "reactor.type";
	static final String STATUS_CANCELLED = MicrometerMeterListener.TAG_STATUS_CANCELLED;
	static final String STATUS_COMPLETED = MicrometerMeterListener.TAG_STATUS_COMPLETED;
	static final String STATUS_COMPLETED_EMPTY = MicrometerMeterListener.TAG_STATUS_COMPLETED_EMPTY;
	static final String STATUS_ERROR = MicrometerMeterListener.TAG_STATUS_ERROR;

	/**
	 * A value for the status tag, to be used when a Mono completes from onNext.
	 * In production, this is set to {@link #STATUS_COMPLETED}.
	 * In some tests, this can be overridden as a way to assert {@link #doOnComplete()} is no-op.
	 */
	final String                                     completedOnNextStatus;
	final MicrometerObservationListenerConfiguration configuration;
	final ContextView                                originalContext;
	final Observation                                subscribeToTerminalObservation;

	@Nullable
	Context contextWithScope;
	@Nullable
	Observation.Scope scope = null;

	boolean valued;

	MicrometerObservationListener(ContextView subscriberContext, MicrometerObservationListenerConfiguration configuration) {
		this(subscriberContext, configuration, STATUS_COMPLETED);
	}

	//for test purposes, we can pass in a value for the status tag, to be used when a Mono completes from onNext
	MicrometerObservationListener(ContextView subscriberContext, MicrometerObservationListenerConfiguration configuration, String completedOnNextStatus) {
		this.configuration = configuration;
		this.originalContext = subscriberContext;
		this.completedOnNextStatus = completedOnNextStatus;

		this.valued = false;

		//creation of the listener matches subscription (Publisher.subscribe(Subscriber) / doFirst)
		//while doOnSubscription matches the moment where the Publisher acknowledges said subscription
		subscribeToTerminalObservation = Observation.createNotStarted(
			configuration.sequenceName + OBSERVATION_SUFFIX_FOR_METRICS,
			configuration.registry
		)
			.contextualName(configuration.sequenceName)
			.lowCardinalityKeyValues(configuration.commonKeyValues);
	}

	@Override
	public void doFirst() {
		ContextSnapshot contextSnapshot = ContextSnapshot.forContextAndThreadLocalValues(this.originalContext);

		try (ContextSnapshot.Scope ignored = contextSnapshot.setThreadLocalValues()) {
			this.scope = this.subscribeToTerminalObservation
				.start()
				.openScope();
			//reacquire the scope from ThreadLocal
			//tap context hasn't been initialized yet, so addToContext can now use the Scope
			ContextSnapshot contextSnapshot2 = ContextSnapshot.forContextAndThreadLocalValues(this.originalContext);
			this.contextWithScope = contextSnapshot2.updateContext(Context.of(this.originalContext));
		}
	}

	@Override
	public Context addToContext(Context originalContext) {
		if (this.originalContext != originalContext) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("addToContext call on Observation {} with unexpected originalContext {}",
					this.subscribeToTerminalObservation, originalContext);
			}
			return originalContext;
		}
		if (this.contextWithScope == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("addToContext call on Observation {} before contextWithScope is set",
				this.subscribeToTerminalObservation);
			}
			return originalContext;
		}
		return contextWithScope;
	}

	@Override
	public void doOnCancel() {
		Observation observation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(KEY_STATUS, STATUS_CANCELLED);

		observation.stop();
		if (scope != null) {
			scope.close();
		}
	}

	@Override
	public void doOnComplete() {
		// We differentiate between empty completion and value completion via tags.
		String status = null;
		if (!valued) {
			status = STATUS_COMPLETED_EMPTY;
		}
		else if (!configuration.isMono) {
			status = STATUS_COMPLETED;
		}

		// if status == null, recording with OnComplete tag is done directly in onNext for the Mono(valued) case
		if (status != null) {
			Observation completeObservation = subscribeToTerminalObservation
				.lowCardinalityKeyValue(KEY_STATUS, status);

			completeObservation.stop();
			if (scope != null) {
				scope.close();
			}
		}
	}

	@Override
	public void doOnError(Throwable e) {
		Observation errorObservation = subscribeToTerminalObservation
			.lowCardinalityKeyValue(KEY_STATUS, STATUS_ERROR)
			.error(e);

		errorObservation.stop();
		if (scope != null) {
			scope.close();
		}
	}

	@Override
	public void doOnNext(T t) {
		valued = true;
		if (configuration.isMono) {
			//record valued completion directly
			Observation completeObservation = subscribeToTerminalObservation
				.lowCardinalityKeyValue(KEY_STATUS, completedOnNextStatus);

			completeObservation.stop();
			if (scope != null) {
				scope.close();
			}
		}
	}

	@Override
	public void handleListenerError(Throwable listenerError) {
		LOGGER.error("unhandled listener error", listenerError);
	}

	//unused hooks

	@Override
	public void doOnSubscription() {
		// NO-OP. We rather initialize everything in `doFirst`, as it is closer to actual Publisher.subscriber call
		// and gives us a chance to store the Scope in the SignalListener's context.
	}

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
}

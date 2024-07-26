/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import io.micrometer.observation.ObservationRegistry;

import reactor.core.observability.SignalListener;
import reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags;
import reactor.core.publisher.SignalType;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.function.Function;

import static reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags.STATUS;
import static reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags.TAG_STATUS_CANCELLED;
import static reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags.TAG_STATUS_COMPLETED;
import static reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags.TAG_STATUS_COMPLETED_EMPTY;
import static reactor.core.observability.micrometer.MicrometerObservationListenerDocumentation.ObservationTags.TAG_STATUS_ERROR;

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

	/**
	 * The key to use to store {@link Observation} in context (same as the one from {@code ObservationThreadLocalAccessor}).
	 *
	 * @implNote this might be redundant, but we got {@code com.sun.tools.javac.code.Symbol$CompletionFailure: class file for io.micrometer.context.ThreadLocalAccessor not found}
	 * in reactor-netty while compiling a similar arrangement. A unit test in MicrometerTest acts as a smoke test in case
	 * micrometer-observation's {@code ObservationThreadLocalAccessor.KEY} changes to something else.
	 */
	static final String CONTEXT_KEY_OBSERVATION = "micrometer.observation";

	/**
	 * A value for the status tag, to be used when a Mono completes from onNext.
	 * In production, this is set to {@link ObservationTags#TAG_STATUS_COMPLETED}.
	 * In some tests, this can be overridden as a way to assert {@link #doOnComplete()} is no-op.
	 */
	final String                                     completedOnNextStatus;
	final MicrometerObservationListenerConfiguration configuration;
	final ContextView originalContext;
	final Observation tapObservation;

	@Nullable
	Context contextWithObservation;

	boolean valued;

	MicrometerObservationListener(ContextView subscriberContext, MicrometerObservationListenerConfiguration configuration) {
		this(subscriberContext, configuration, null);
	}

	MicrometerObservationListener(ContextView subscriberContext,
			MicrometerObservationListenerConfiguration configuration,
			@Nullable Function<ObservationRegistry, Observation> observationSupplier) {
		this(subscriberContext, configuration, TAG_STATUS_COMPLETED, observationSupplier);
	}

	//for test purposes, we can pass in a value for the status tag, to be used when a Mono completes from onNext
	MicrometerObservationListener(ContextView subscriberContext,
			MicrometerObservationListenerConfiguration configuration,
			String completedOnNextStatus,
			@Nullable Function<ObservationRegistry, Observation> observationSupplier) {
		this.configuration = configuration;
		this.originalContext = subscriberContext;
		this.completedOnNextStatus = completedOnNextStatus;

		this.valued = false;

		//creation of the listener matches subscription (Publisher.subscribe(Subscriber) / doFirst)
		//while doOnSubscription matches the moment where the Publisher acknowledges said subscription
		//NOTE: we don't use the `DocumentedObservation` features to create the Observation, even for the ANONYMOUS case,
		//because the discovered tags could be more than the documented defaults
		tapObservation = supplyOrCreateObservation(configuration, observationSupplier)
				.lowCardinalityKeyValues(configuration.commonKeyValues);
	}

	private static Observation supplyOrCreateObservation(
			MicrometerObservationListenerConfiguration configuration,
			@Nullable Function<ObservationRegistry, Observation> observationSupplier) {
		if (observationSupplier != null) {
			final Observation observation = observationSupplier.apply(configuration.registry);
			if (observation != null) {
				if (observation.getContext().getContextualName() != null) {
					return observation;
				}
				else {
					return observation.contextualName(configuration.sequenceName);
				}
			}
		}
		return Observation.createNotStarted(configuration.sequenceName, configuration.registry)
		                  .contextualName(configuration.sequenceName);
	}

	@Override
	public void doFirst() {
		/* Implementation note on using parentObservation vs openScope:
		Opening a Scope is never necessary in this tap listener, because the Observation we create is stored in
		the Context the tap operator will expose to upstream, rather than via ThreadLocal population.

		We also make a best-effort attempt to discover such an Observation in the context here in doFirst, so that this
		can explicitly be used as the parentObservation. At this point, if none is found we take also the opportunity
		of checking if the registry has a currentObservation.

		As a consequence, fanout (eg. with a `flatMap`) upstream of the tap should be able to see the current Observation
		in the context and the inner publishers should inherit it as their parent observation if they also use `tap(Micrometer.observation())`.

		Note that Reactor's threading model doesn't generally guarantee that doFirst and doOnNext/doOnComplete/doOnError run
		in the same thread, and that's the main reason why Scopes are avoided here (as their sole purpose is to set up
		Thread Local variables).
		 */

		Observation o;
		Observation p;
		if (this.originalContext.hasKey(CONTEXT_KEY_OBSERVATION)) {
			p = this.originalContext.get(CONTEXT_KEY_OBSERVATION);
		}
		else {
			p = this.configuration.registry.getCurrentObservation();
		}

		if (p != null) {
			o = this.tapObservation
				.parentObservation(p)
				.start();
		}
		else {
			o = this.tapObservation.start();
		}
		this.contextWithObservation = Context.of(this.originalContext)
			.put(CONTEXT_KEY_OBSERVATION, o);
	}

	@Override
	public Context addToContext(Context originalContext) {
		if (this.originalContext != originalContext) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("addToContext call on Observation {} with unexpected originalContext {}",
					this.tapObservation, originalContext);
			}
			return originalContext;
		}
		if (this.contextWithObservation == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("addToContext call on Observation {} before contextWithScope is set",
				this.tapObservation);
			}
			return originalContext;
		}
		return contextWithObservation;
	}

	@Override
	public void doOnCancel() {
		Observation observation = tapObservation
			.lowCardinalityKeyValue(STATUS.asString(), TAG_STATUS_CANCELLED);

		observation.stop();
	}

	@Override
	public void doOnComplete() {
		// We differentiate between empty completion and value completion via tags.
		String status = null;
		if (!valued) {
			status = TAG_STATUS_COMPLETED_EMPTY;
		}
		else if (!configuration.isMono) {
			status = TAG_STATUS_COMPLETED;
		}

		// if status == null, recording with OnComplete tag is done directly in onNext for the Mono(valued) case
		if (status != null) {
			Observation completeObservation = tapObservation
				.lowCardinalityKeyValue(STATUS.asString(), status);

			completeObservation.stop();
		}
	}

	@Override
	public void doOnError(Throwable e) {
		Observation errorObservation = tapObservation
			.lowCardinalityKeyValue(STATUS.asString(), TAG_STATUS_ERROR)
			.error(e);

		errorObservation.stop();
	}

	@Override
	public void doOnNext(T t) {
		valued = true;
		if (configuration.isMono) {
			//record valued completion directly
			Observation completeObservation = tapObservation
				.lowCardinalityKeyValue(STATUS.asString(), completedOnNextStatus);

			completeObservation.stop();
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

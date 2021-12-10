/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.subscriber;

import java.util.function.Predicate;

import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A configuration builder used to create a fine-tuned {@link TestSubscriber}, also
 * allowing for a {@link reactor.core.Fuseable.ConditionalSubscriber} variant.
 * Notably, it allows the created instance to enforce fusion compatibility
 * (by assuming a {@link Fuseable} source publisher and expecting a
 * {@link reactor.core.Fuseable.QueueSubscription}).
 * <p>
 * See {@link TestSubscriber#builder()} to obtain a new builder.
 * <p>
 * Note that all methods mutate the builder configuration, so reuse is discouraged.
 *
 * @author Simon Baslé
 */
public final class TestSubscriberBuilder {

	long                             initialRequest;
	Context                                 context;
	TestSubscriber.FusionRequirement fusionRequirement;
	int                                     requestedFusionMode;
	int                              expectedFusionMode;

	TestSubscriberBuilder() {
		this.initialRequest = Long.MAX_VALUE;
		this.context = Context.empty();
		this.fusionRequirement = TestSubscriber.FusionRequirement.NONE;
		this.requestedFusionMode = Fuseable.NONE;
		this.expectedFusionMode = Fuseable.NONE;
	}

	/**
	 * Enrich the {@link Context} with a single entry.
	 *
	 * @param key the key to put/set in the {@link Context} of the future {@link TestSubscriber}
	 * @param value the value to associate with the key
	 * @return this builder, mutated to have the key/value pair added to the {@link Context}
	 */
	public TestSubscriberBuilder contextPut(Object key, Object value) {
		this.context = context.put(key, value);
		return this;
	}

	/**
	 * Enrich the {@link Context} by putting all entries of the given {@link ContextView} in it.
	 *
	 * @param toAdd the {@link ContextView} to add to the {@link Context} of the future {@link TestSubscriber}
	 * @return this builder, mutated to have all {@link ContextView}'s key/value pairs added to the {@link Context}
	 */
	public TestSubscriberBuilder contextPutAll(ContextView toAdd) {
		this.context = context.putAll(toAdd);
		return this;
	}

	/**
	 * Set the request to be made upon receiving the {@link org.reactivestreams.Subscription}
	 * in {@link org.reactivestreams.Subscriber#onSubscribe(Subscription)}. Default is {@link Long#MAX_VALUE}.
	 *
	 * @param initialRequest the request to be made at subscription
	 * @return this builder, mutated to reflect the new request to be made at subscription
	 */
	public TestSubscriberBuilder initialRequest(long initialRequest) {
		this.initialRequest = initialRequest;
		return this;
	}

	/**
	 * Set the request to be made upon receiving the {@link org.reactivestreams.Subscription}
	 * in {@link org.reactivestreams.Subscriber#onSubscribe(Subscription)} to be an
	 * unbounded request, ie. {@link Long#MAX_VALUE}.
	 *
	 * @return this builder, mutated to reflect an unbounded request is to be made at subscription
	 */
	public TestSubscriberBuilder initialRequestUnbounded() {
		this.initialRequest = Long.MAX_VALUE;
		return this;
	}

	/**
	 * Expect fusion to be possible with the {@link TestSubscriber}, with {@link Fuseable#NONE}) being a special case.
	 * Fusion will be negotiated at subscription, enforcing the need for a {@link reactor.core.Fuseable.QueueSubscription} to be passed.
	 * Furthermore, the fusion mode returned from the negotiation with the {@link reactor.core.Fuseable.QueueSubscription}
	 * is expected to be the same as the provided mode.
	 * <p>
	 * Use {@code requireFusion(Fuseable.NONE)} to remove any previously fusion requirement, as well as
	 * removing type enforcement on the subscription (a vanilla {@link Subscription} becomes acceptable).
	 * Use {@link #requireNotFuseable()} to strictly enforce that the {@link Subscription} MUST NOT be a {@link reactor.core.Fuseable.QueueSubscription}.
	 *
	 * @param exactMode the requested fusion mode, expected in return from the negotiation with the {@link reactor.core.Fuseable.QueueSubscription}
	 * @return this builder, mutated to require fusion with the given mode
	 */
	public TestSubscriberBuilder requireFusion(int exactMode) {
		return requireFusion(exactMode, exactMode);
	}

	/**
	 * Expect fusion to be possible with the {@link TestSubscriber}, with both parameters set to {@link Fuseable#NONE} being a special case.
	 * Fusion will be negotiated at subscription, enforcing the need for a {@link reactor.core.Fuseable.QueueSubscription} to be passed.
	 * Furthermore, the {@code negotiatedMode} is expected to be negotiated by the subscription
	 * in response to requesting {@code requestedMode}.
	 * <p>
	 * Use {@code requireFusion(Fuseable.NONE, Fuseable.NONE)} to remove any previously fusion requirement, as well as
	 * removing type enforcement on the subscription (a vanilla {@link Subscription} becomes acceptable).
	 * Use {@link #requireNotFuseable()} to strictly enforce that the {@link Subscription} MUST NOT be a {@link reactor.core.Fuseable.QueueSubscription}.
	 *
	 * @param requestedMode the fusion mode requested to the {@link reactor.core.Fuseable.QueueSubscription}
	 * @param negotiatedMode the fusion mode expected from the negotiation with the {@link reactor.core.Fuseable.QueueSubscription}
	 * @return this builder, mutated to require fusion with the given negotiated mode (in response to the given requested mode)
	 */
	public TestSubscriberBuilder requireFusion(int requestedMode, int negotiatedMode) {
		if (requestedMode == negotiatedMode && negotiatedMode == Fuseable.NONE) {
			this.fusionRequirement = TestSubscriber.FusionRequirement.NONE;
		}
		else {
			this.fusionRequirement = TestSubscriber.FusionRequirement.FUSEABLE;
		}
		this.requestedFusionMode = requestedMode;
		this.expectedFusionMode = negotiatedMode;
		return this;
	}

	/**
	 * Enforce that the {@link Subscription} passed to the {@link TestSubscriber} isn't a {@link reactor.core.Fuseable.QueueSubscription}.
	 * This is stricter than {@code requireFusion(Fuseable.NONE, Fuseable.NONE)}, which merely disables fusion but still accepts
	 * the incoming subscription to be a QueueSubscription.
	 *
	 * @return this builder, mutated to reject all {@link reactor.core.Fuseable.QueueSubscription}
	 */
	public TestSubscriberBuilder requireNotFuseable() {
		this.fusionRequirement = TestSubscriber.FusionRequirement.NOT_FUSEABLE;
		this.requestedFusionMode = Fuseable.NONE;
		this.expectedFusionMode = Fuseable.NONE;
		return this;
	}

	/**
	 * Create a {@link reactor.core.Fuseable.ConditionalSubscriber} variant of {@link TestSubscriber} according
	 * to this builder.
	 * The provided {@link Predicate} will be used as the implementation
	 * of {@link reactor.core.Fuseable.ConditionalSubscriber#tryOnNext(Object)}.
	 *
	 * @param tryOnNext the {@link Predicate} to use as the {@link reactor.core.Fuseable.ConditionalSubscriber#tryOnNext tryOnNext} implementation
	 * @param <T> the type of elements received by the {@link TestSubscriber}, defined by the predicate
	 * @return a {@link ConditionalTestSubscriber}
	 */
	public <T> ConditionalTestSubscriber<T> buildConditional(Predicate<? super T> tryOnNext) {
		return new DefaultConditionalTestSubscriber<T>(this, tryOnNext);
	}

	/**
	 * Create a {@link TestSubscriber} according to this builder.
	 *
	 * @param <T> the type of elements to be received by the subscriber
	 * @return a new plain {@link TestSubscriber}
	 */
	public <T> TestSubscriber<T> build() {
		return new DefaultTestSubscriber<>(this);
	}
}

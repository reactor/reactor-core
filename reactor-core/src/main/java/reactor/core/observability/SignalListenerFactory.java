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

package reactor.core.observability;

import org.reactivestreams.Publisher;

import reactor.util.context.ContextView;

/**
 * A factory for per-subscription {@link SignalListener}, exposing the ability to generate common state at publisher level
 * from the source {@link Publisher}.
 * <p>
 * Examples of such state include:
 * <ul>
 *     <li>is the publisher a Mono? (unlocking Mono-specific behavior in the {@link SignalListener}</li>
 *     <li>resolution of NAME and TAGS on the source</li>
 *     <li>preparation of a common configuration, like a registry for metrics</li>
 * </ul>
 *
 * @param <T> the type of data emitted by the observed source {@link Publisher}
 * @param <STATE> the type of the publisher-level state that will be shared between all {@link SignalListener} created by this factory
 * @author Simon Baslé
 */
public interface SignalListenerFactory<T, STATE> {

	/**
	 * Create the {@code STATE} object for the given {@link Publisher}. This assumes this factory will only be used on
	 * that particular source, allowing all {@link SignalListener} created by this factory to inherit the common state.
	 *
	 * @param source the source {@link Publisher} this factory is attached to.
	 * @return the common state
	 */
	STATE initializePublisherState(Publisher<? extends T> source);

	/**
	 * Create a new {@link SignalListener} each time a new {@link org.reactivestreams.Subscriber} subscribes to the
	 * {@code source} {@link Publisher}.
	 * <p>
	 * The {@code source} {@link Publisher} is the same as the one that triggered common state creation at assembly time in
	 * {@link #initializePublisherState(Publisher)}). Said common state is passed to this method as well, and so is the
	 * {@link ContextView} for the newly registered {@link reactor.core.CoreSubscriber}.
	 *
	 * @param source the source {@link Publisher} that is being subscribed to
	 * @param listenerContext the {@link ContextView} associated with the new subscriber
	 * @param publisherContext the common state initialized in {@link #initializePublisherState(Publisher)}
	 * @return a stateful {@link SignalListener} observing signals to and from the new subscriber
	 */
	SignalListener<T> createListener(Publisher<? extends T> source, ContextView listenerContext, STATE publisherContext);

}

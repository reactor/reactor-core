/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * @author Simon Baslé
 */
public interface FluxProcessorFacade<T> extends ProcessorFacade<T> {

	/**
	 * Expose a Reactor {@link Flux} API on top of the {@link FluxProcessorSink}'s output,
	 * allowing composition of operators on it.
	 *
	 * @implNote most implementations will already implement {@link Flux}
	 * and thus can return themselves.
	 *
	 * @return a full reactive {@link Flux} API on top of the {@link FluxProcessorSink}'s output
	 */
	Flux<T> asFlux();

	/**
	 * Return a view of this {@link FluxProcessorFacade} that is a
	 * {@link Processor Processor&lt;T, T&gt;}, suitable for subscribing to a source
	 * {@link Publisher}.
	 *
	 * @return the {@link Processor} backing this {@link ProcessorFacade}
	 */
	Processor<T, T> asProcessor();

	/**
	 * Return a view of this {@link FluxProcessorFacade} that is a {@link CoreSubscriber},
	 * suitable for subscribing to a source {@link Publisher}.
	 *
	 * @implSpec if the backing {@link Processor} doesn't come from Reactor, this method
	 * should return it wrapped in a {@link CoreSubscriber} adapter.
	 * @return the {@link CoreSubscriber} backing this {@link ProcessorFacade}
	 */
	CoreSubscriber<T> asCoreSubscriber();

}

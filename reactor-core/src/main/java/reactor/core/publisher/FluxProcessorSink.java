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

import java.util.function.Consumer;

/**
 * A standalone {@link FluxSink}, similar to a {@link FluxProcessorFacade} except tailored
 * for manual triggering of events rather than {@link org.reactivestreams.Processor}-like
 * subscribe-and-emit use.
 *
 * @author Simon Baslé
 */
public interface FluxProcessorSink<T> extends ProcessorFacade<T>, FluxSink<T> {

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
	 * Terminate with the given exception
	 * <p>
	 * Calling this method multiple times or after the other terminating methods is
	 * an unsupported operation. It will discard the exception through the
	 * {@link Hooks#onErrorDropped(Consumer)} hook (which by default throws the exception
	 * wrapped via {@link reactor.core.Exceptions#bubble(Throwable)}). This is to avoid
	 * complete and silent swallowing of the exception.
	 *
	 * @param e the exception to complete with
	 */
	@Override
	void error(Throwable e);

	/**
	 * @return the {@link FluxSink.OverflowStrategy} that was used when instantiating this {@link FluxProcessorSink}
	 */
	OverflowStrategy getOverflowStrategy();

}

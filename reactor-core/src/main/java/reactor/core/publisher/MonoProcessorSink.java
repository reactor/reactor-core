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

/**
 * A standalone {@link MonoSink}, similar to {@link MonoProcessorFacade} except tailored
 * for manual triggering of events rather than {@link org.reactivestreams.Processor}-like
 * subscribe-and-emit use.
 *
 * @author Simon Baslé
 */
public interface MonoProcessorSink<T> extends ProcessorFacade<T>, MonoSink<T> {

	/**
	 * Expose a Reactor {@link Mono} API on top of the {@link MonoProcessorSink}'s output,
	 * allowing composition of operators on it.
	 *
	 * @implNote most implementations will already implement {@link Mono}
	 * and thus can return themselves.
	 *
	 * @return a full reactive {@link Mono} API on top of the {@link MonoProcessorSink}'s output
	 */
	Mono<T> asMono();

	/**
	 * Indicates whether this {@link MonoProcessorSink} has completed with or without a value,
	 * whereas {@link #isValued()} indicates it completed with a value.
	 *
	 * @return {@code true} if this {@link MonoProcessorSink} is completed, but without value, {@code false} otherwise.
	 */
	@Override
	boolean isComplete();

	/**
	 * Indicates whether this {@link MonoProcessorSink} has completed with a value,
	 * whereas {@link #isComplete()} indicates it completed, but could be without a value.
	 *
	 * @return {@code true} if this {@link MonoProcessorSink} is completed with value, {@code false} otherwise.
	 */
	boolean isValued();

}

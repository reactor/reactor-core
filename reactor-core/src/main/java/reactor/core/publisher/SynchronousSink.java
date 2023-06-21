/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.function.Function;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Interface to produce synchronously "one signal" to an underlying {@link Subscriber}.
 * <p>
 *     At most one {@link #next} call and/or one {@link #complete()} or
 *     {@link #error(Throwable)} should be called per invocation of the generator function.
 *
 * <p>
 *     Calling a {@link SynchronousSink} outside of a generator consumer or function, e.g.
 *     using an async callback, is forbidden. You can {@link FluxSink} or
 *     {@link MonoSink} based generators for these situations.
 *
 * @param <T> the output value type
 */
public interface SynchronousSink<T> {
	/**
	 * @see Subscriber#onComplete()
	 */
	void complete();

	/**
	 * Return the current subscriber {@link Context}.
	 * <p>
	 *   {@link Context} can be enriched via {@link Mono#contextWrite(Function)}
	 *   or {@link Flux#contextWrite(Function)}
	 *   operator or directly by a child subscriber overriding
	 *   {@link CoreSubscriber#currentContext()}
	 *
	 * @return the current subscriber {@link Context}.
	 * @deprecated To be removed in 3.6.0 at the earliest. Prefer using #contextView() instead.
	 */
	@Deprecated
	Context currentContext();

	/**
	 * Return the current subscriber's context as a {@link ContextView} for inspection.
	 * <p>
	 *   {@link Context} can be enriched downstream via {@link Mono#contextWrite(Function)}
	 *   or {@link Flux#contextWrite(Function)} operators or directly by a child subscriber overriding
	 *   {@link CoreSubscriber#currentContext()}
	 *
	 * @return the current subscriber {@link ContextView}.
	 */
	default ContextView contextView() {
		return currentContext();
	}

	/**
	 * @param e the exception to signal, not null
	 *
	 * @see Subscriber#onError(Throwable)
	 */
	void error(Throwable e);

	/**
	 * Try emitting, might throw an unchecked exception.
	 *
	 * @param t the value to emit, not null
	 *
	 * @throws RuntimeException in case of unchecked error during the emission
	 * @see Subscriber#onNext(Object)
	 */
	void next(T t);
}

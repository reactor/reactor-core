/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Subscriber;

/**
 * A base interface for a "sequence" sink, a construct through which Reactive Streams
 * signals can be programmatically pushed with generic publisher (or {@link Flux})
 * semantics.
 * <p>
 * Sinks can be used in the context of an operator that ties one sink per {@link Subscriber}
 * (thus allowing pushing data to a specific {@link Subscriber}, see {@link FluxSink})
 * or encourage fully standalone usage (see {@link reactor.core.publisher.Sinks.StandaloneFluxSink}).
 *
 * @author Simon Baslé
 */
public interface SequenceSink<T> {

	/**
	 * @see Subscriber#onComplete()
	 */
	void complete();


	/**
	 * @see Subscriber#onError(Throwable)
	 * @param e the exception to signal, not null
	 */
	void error(Throwable e);

	/**
	 * Try emitting, might throw an unchecked exception.
	 * @see Subscriber#onNext(Object)
	 * @param t the value to emit, not null
	 */
	SequenceSink<T> next(T t);

}

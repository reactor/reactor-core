/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
 * Interface to generate signals to a bridged {@link Subscriber}.
 * <p>
 * At most one {@link #next} call and/or one {@link #complete()} or {@link
 * #error(Throwable)} should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SynchronousSink<T> {

	/**
	 * @see Subscriber#onComplete()
	 */
	void complete();

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

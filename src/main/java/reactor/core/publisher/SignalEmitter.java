/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import org.reactivestreams.Subscriber;
import reactor.core.Trackable;

/**
 * Interface to generate signals to a bridged {@link Subscriber}.
 * <p>
 * At least one of the methods should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SignalEmitter<T> extends Trackable {

	/**
	 * @see Subscriber#onComplete()
	 */
	void complete();

	/**
	 * @see Subscriber#onError(Throwable)
	 * @param e the exception to signal, not null
	 */
	void fail(Throwable e);

	/**
	 * Try emitting, might throw an unchecked exception.
	 * @see Subscriber#onNext(Object)
	 * @param t the value to emit, not null
	 * @throws RuntimeException
	 */
	void next(T t);
}

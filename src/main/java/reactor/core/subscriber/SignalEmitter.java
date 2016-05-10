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
package reactor.core.subscriber;

import org.reactivestreams.Subscriber;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Introspectable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Interface to generate signals to a bridged {@link Subscriber}.
 * <p>
 * At least one of the methods
 * should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SignalEmitter<T> extends Backpressurable, Introspectable, Cancellable,
                                          Requestable {

	/**
	 * @see Subscriber#onComplete()
	 */
	void complete();

	/**
	 * @see Subscriber#onError(Throwable)
	 */
	void fail(Throwable e);

	/**
	 * Try emitting, might throw an unchecked exception.
	 * @see Subscriber#onNext(Object)
	 *
	 * @throws RuntimeException
	 */
	void next(T t);

	/**
	 * Indicate there won't be any further signals delivered by
	 * the generator and the operator will stop calling it.
	 * <p>
	 * Call to this method will also trigger the state consumer.
	 */
	void stop();
}

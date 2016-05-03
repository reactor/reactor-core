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

/**
 * Interface to receive generated signals from the callback function.
 * <p>
 * Methods of this interface should be called at most once per invocation
 * of the generator function. In addition, at least one of the methods
 * should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface GenerateOutput<T> {

	/**
	 * @see {@link Subscriber#onNext(Object)}
	 */
	void onNext(T t);

	/**
	 * @see {@link Subscriber#onError(Throwable)}
	 */
	void onError(Throwable e);

	/**
	 * @see {@link Subscriber#onComplete()}
	 */
	void onComplete();

	/**
	 * Indicate there won't be any further signals delivered by
	 * the generator and the operator will stop calling it.
	 * <p>
	 * Call to this method will also trigger the state consumer.
	 */
	void stop();
}

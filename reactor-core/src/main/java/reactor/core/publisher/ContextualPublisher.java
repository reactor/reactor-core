/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.context.Context;

/**
 *
 * An {@link Publisher} marker for Reactor {@link Mono} and {@link Flux} extended
 * subscribe lifecycle. In addition to behave as expected by
 * {@link Publisher#subscribe(Subscriber)}, it supports {@link Context} passing.
 *
 * @param <T> a Subscriber observed sequence type
 *
 * @author Stephane Maldini
 */
public interface ContextualPublisher<T> extends Publisher<T> {

	/**
	 * An internal {@link Publisher#subscribe(Subscriber)} implemented by
	 * both reactive sources {@link Flux} and {@link Mono}.
	 * <p>
	 * In addition to behave as expected by {@link Publisher#subscribe(Subscriber)}
	 * in a controlled manner, it supports {@link Context} passing.
	 *
	 * @param actual the {@link Subscriber} interested into the published sequence
	 * @param context a {@link Context} to provide to the operational chain.
	 *
	 * @see Publisher#subscribe(Subscriber)
	 */
	void subscribe(Subscriber<? super T> actual, Context context);
}

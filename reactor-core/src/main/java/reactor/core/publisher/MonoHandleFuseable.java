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

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 * <p>
 * This variant allows composing fuseable stages.
 * 
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoHandleFuseable<T, R> extends MonoOperator<T, R>
		implements Fuseable {

	final BiConsumer<? super T, SynchronousSink<R>>           handler;
	@Nullable
	final BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler;

	/**
	 * @param source the source
	 * @param handler the handler function
	 * @param terminateHandler the optional terminate function that can optionally change
	 * 	 * the terminate signal)
	 */
	MonoHandleFuseable(Mono<? extends T> source, BiConsumer<? super T, SynchronousSink<R>> handler,
			@Nullable BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
		this.terminateHandler = terminateHandler;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super R> actual) {
		source.subscribe(new FluxHandleFuseable.HandleFuseableSubscriber<>(actual, handler,
				terminateHandler));
	}

}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.BiConsumer;

import reactor.core.CoreSubscriber;

/**
 * Maps the values of the source publisher one-on-one via a mapper function. If the result is not {code null} then the
 * {@link Mono} will complete with this value.  If the result of the function is {@code null} then the {@link Mono}
 * will complete without a value.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoHandle<T, R> extends InternalMonoOperator<T, R> {

	final BiConsumer<? super T, SynchronousSink<R>> handler;

	MonoHandle(Mono<? extends T> source, BiConsumer<? super T, SynchronousSink<R>> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		return new FluxHandle.HandleSubscriber<>(actual, handler);
	}
}

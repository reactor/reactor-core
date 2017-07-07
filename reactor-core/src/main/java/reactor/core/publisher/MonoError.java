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

import java.time.Duration;
import java.util.Objects;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoError<T> extends Mono<T> {

	final Throwable error;

	MonoError(Throwable error) {
		this.error = Objects.requireNonNull(error, "error");
	}

	@Override
	public T block(Duration m) {
		throw Exceptions.propagate(error);
	}

	@Override
	public T block() {
		throw Exceptions.propagate(error);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		Operators.error(s, Operators.onOperatorError(error));
	}
}

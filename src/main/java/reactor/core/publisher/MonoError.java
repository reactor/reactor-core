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

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoError<T> extends Mono<T> implements Trackable {

	final Throwable error;

	MonoError(Throwable error) {
		this.error = Objects.requireNonNull(error, "error");
	}

	@Override
	public Throwable getError() {
		return error;
	}

	@Override
	public T blockMillis(long m) {
		throw Exceptions.propagate(getError());
	}

	@Override
	public T block() {
		throw Exceptions.propagate(getError());
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Operators.error(s, Operators.onOperatorError(error));
	}

	@Override
	public boolean isTerminated() {
		return true;
	}
}

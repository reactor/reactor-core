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
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
final class MonoError<T> 
extends Mono<T> implements reactor.core.state.Completable {

	final Supplier<? extends Throwable> supplier;

	public MonoError(Throwable error) {
		this(create(error));
	}

	static Supplier<Throwable> create(final Throwable error) {
		Objects.requireNonNull(error);
		return () -> error;
	}

	public MonoError(Supplier<? extends Throwable> supplier) {
		this.supplier = Objects.requireNonNull(supplier);
	}

	@Override
	public Throwable getError() {
		return supplier.get();
	}

	@Override
	public T get() {
		throw Exceptions.propagate(getError());
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Throwable e;

		try {
			e = supplier.get();
		} catch (Throwable ex) {
			e = ex;
		}

		if (e == null) {
			e = new NullPointerException("The Throwable returned by the supplier is null");
		}

		EmptySubscription.error(s, e);
	}

	@Override
	public boolean isTerminated() {
		return true;
	}
}

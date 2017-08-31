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
import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link java.util.function.Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoCallable<T> 
extends Mono<T>
		implements Callable<T>, Fuseable {

	final Callable<? extends T> callable;

	MonoCallable(Callable<? extends T> callable) {
		this.callable = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {

		Operators.MonoSubscriber<T, T>
				sds = new Operators.MonoSubscriber<>(actual);

		actual.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		T t;
		try {
			t = Objects.requireNonNull(callable.call(), "callable returned null");
		}
		catch (Throwable e) {
			actual.onError(Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		sds.complete(t);
	}

	@Override
	public T block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	public T block(Duration m) {
		try {
			return Objects.requireNonNull(callable.call(),
					"The callable source returned null");
		}
		catch (Throwable e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException)e;
			}
			throw Exceptions.propagate(e);
		}
	}

	@Override
	public T call() throws Exception {
		return Objects.requireNonNull(callable.call(),
				"The callable source returned null");
	}
}

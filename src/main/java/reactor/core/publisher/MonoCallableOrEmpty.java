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
import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.util.Exceptions;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class MonoCallableOrEmpty<T>
extends Mono<T>
		implements Receiver, Callable<T>, Fuseable {

	final Callable<? extends T> callable;

	public MonoCallableOrEmpty(Callable<? extends T> callable) {
		this.callable = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public Object upstream() {
		return callable;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		OperatorHelper.DeferredScalarSubscriber<T, T>
				sds = new OperatorHelper.DeferredScalarSubscriber<>(s);

		s.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		T t;
		try {
			t = callable.call();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			s.onError(Exceptions.unwrap(e));
			return;
		}

		if (t == null) {
			s.onComplete();
			return;
		}

		sds.complete(t);
	}
	
	@Override
	public T block() {
		try {
			return callable.call();
		} catch (Throwable e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException)e;
			}
			throw Exceptions.bubble(e);
		}
	}

	@Override
	public T call() throws Exception {
	    return callable.call();
	}
}

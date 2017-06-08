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
import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;


/**
 * For each subscriber, a Supplier is invoked and the returned value emitted.
 *
 * @param <T> the value type;
 */
final class FluxCallable<T> extends Flux<T> implements Callable<T>, Fuseable {

	final Callable<T> callable;

	FluxCallable(Callable<T> callable) {
		this.callable = callable;
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context context) {
		Operators.MonoSubscriber<T, T> wrapper = new Operators.MonoSubscriber<>(s);
		s.onSubscribe(wrapper);

		T v;
		try {
			v = Objects.requireNonNull(callable.call(), "callable returned null");
		}
		catch (Throwable ex) {
			s.onError(Operators.onOperatorError(ex));
			return;
		}

		wrapper.complete(v);
	}

	@Override
	public T call() throws Exception {
		return callable.call();
	}
}

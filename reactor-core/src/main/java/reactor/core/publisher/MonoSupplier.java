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
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

/**
 * Executes a Supplier function and emits a single value to each individual Subscriber.
 *
 * @param <T> the returned value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSupplier<T> 
extends Mono<T>
		implements Callable<T>, Fuseable {

	final Supplier<? extends T> supplier;

	MonoSupplier(Supplier<? extends T> callable) {
		this.supplier = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {

		Operators.MonoSubscriber<T, T>
				sds = new Operators.MonoSubscriber<>(s);

		s.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		T t;
		try {
			t = Objects.requireNonNull(supplier.get(),
					"The supplier source returned null");
		}
		catch (Throwable e) {
			s.onError(Operators.onOperatorError(e));
			return;
		}

		sds.complete(t);
	}
	
	@Override
	public T block(Duration m) {
		return Objects.requireNonNull(supplier.get(),
				"The supplier source returned null");
	}

	@Override
	public T block() {
		//the duration is ignored above
		return block(Duration.ZERO);
	}
	
	@Override
	public T call() throws Exception {
		return Objects.requireNonNull(supplier.get(),
				"The supplier source returned null");
	}
}

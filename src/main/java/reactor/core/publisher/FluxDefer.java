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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Exceptions;
import reactor.core.Receiver;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDefer<T> extends Flux<T>
		implements Receiver {

	final Supplier<? extends Publisher<? extends T>> supplier;

	public FluxDefer(Supplier<? extends Publisher<? extends T>> supplier) {
		this.supplier = Objects.requireNonNull(supplier, "supplier");
	}

	@Override
	public Object upstream() {
		return supplier;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Publisher<? extends T> p;

		try {
			p = supplier.get();
		} catch (Throwable e) {
			Operators.error(s, Exceptions.mapOperatorError(e));
			return;
		}

		if (p == null) {
			Operators.error(s,
					Exceptions.mapOperatorError(new NullPointerException(
							"The Producer returned by the supplier is null")));
			return;
		}

		p.subscribe(s);
	}
}

/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import reactor.core.CoreSubscriber;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDefer<T> extends Mono<T> implements SourceProducer<T> {

	final Supplier<? extends Mono<? extends T>> supplier;

	MonoDefer(Supplier<? extends Mono<? extends T>> supplier) {
		this.supplier = Objects.requireNonNull(supplier, "supplier");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Mono<? extends T> p;

		try {
			p = Objects.requireNonNull(supplier.get(),
					"The Mono returned by the supplier is null");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		fromDirect(p).subscribe(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return SourceProducer.super.scanUnsafe(key);
	}
}

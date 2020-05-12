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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDefer<T> extends Flux<T> implements SourceProducer<T>, OptimizableOperator<T, T> {

	final Supplier<? extends Publisher<? extends T>> supplier;
	Publisher<? extends T> resolvedPublisher;

	FluxDefer(Supplier<? extends Publisher<? extends T>> supplier) {
		this.supplier = Objects.requireNonNull(supplier, "supplier");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		try {
			Objects.requireNonNull(resolvedPublisher, "The Publisher returned by the supplier is null");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		from(this.resolvedPublisher).subscribe(actual);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual)
			throws Throwable {
		return actual;
	}

	@Override
	public CorePublisher<? extends T> source() {
		return this;
	}

	@Override
	public OptimizableOperator<?, ? extends T> nextOptimizableSource() {
		this.resolvedPublisher = supplier.get();
		if (this.resolvedPublisher instanceof OptimizableOperator) {
			//noinspection unchecked
			return (OptimizableOperator<?, ? extends T>) this.resolvedPublisher;
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}
}

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.FluxCreate.BaseSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;

/**
 * Provides a multi-valued sink API for a callback that is called for each individual
 * Subscriber, in a single-threaded producer fashion.
 *
 * @param <T> the value type
 */
final class FluxCreatePush<T> extends Flux<T> implements SourceProducer<T> {

	final Consumer<? super FluxSink<T>> source;

	final OverflowStrategy backpressure;

	FluxCreatePush(Consumer<? super FluxSink<T>> source,
			OverflowStrategy backpressure) {
		this.source = Objects.requireNonNull(source, "source");
		this.backpressure = Objects.requireNonNull(backpressure, "backpressure");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		BaseSink<T> sink = FluxCreate.createSink(actual, backpressure);

		actual.onSubscribe(sink);
		try {
			source.accept(sink);
		}
		catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			sink.error(Operators.onOperatorError(ex, actual.currentContext()));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

}

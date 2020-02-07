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

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Emits the contents of a Stream source.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxStream<T> extends Flux<T> implements Fuseable, SourceProducer<T> {

	final Supplier<? extends Stream<? extends T>> streamSupplier;

	FluxStream(Supplier<? extends Stream<? extends T>> streamSupplier) {
		this.streamSupplier = Objects.requireNonNull(streamSupplier, "streamSupplier");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Stream<? extends T> stream;
		try {
			stream = Objects.requireNonNull(streamSupplier.get(),
					"The stream supplier returned a null Stream");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		Iterator<? extends T> it;
		boolean knownToBeFinite;
		try {
			Spliterator<? extends T> spliterator = Objects.requireNonNull(stream.spliterator(),
					"The stream returned a null Spliterator");
			knownToBeFinite = spliterator.hasCharacteristics(Spliterator.SIZED);
			it = Spliterators.iterator(spliterator); //this is the default for BaseStream#iterator() anyway
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		//although not required by AutoCloseable, Stream::close SHOULD be idempotent
		//(at least the default AbstractPipeline implementation is)
		FluxIterable.subscribe(actual, it, knownToBeFinite, stream::close);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}
}

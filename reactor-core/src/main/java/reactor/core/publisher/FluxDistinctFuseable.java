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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxDistinct.DistinctFuseableSubscriber;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extracted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDistinctFuseable<T, K, C extends Collection<? super K>>
		extends FluxOperator<T, T> implements Fuseable {

	final Function<? super T, ? extends K> keyExtractor;

	final Supplier<C> collectionSupplier;

	FluxDistinctFuseable(Flux<? extends T> source,
			Function<? super T, ? extends K> keyExtractor,
			Supplier<C> collectionSupplier) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier =
				Objects.requireNonNull(collectionSupplier, "collectionSupplier");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		C collection;

		try {
			collection = Objects.requireNonNull(collectionSupplier.get(),
					"The collectionSupplier returned a null collection");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		source.subscribe(new DistinctFuseableSubscriber<>(actual, collection, keyExtractor));
	}
}

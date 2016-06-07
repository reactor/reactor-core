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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.flow.Fuseable;
import reactor.core.publisher.FluxDistinct.DistinctFuseableSubscriber;
import reactor.core.util.EmptySubscription;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxDistinctFuseable<T, K, C extends Collection<? super K>> 
extends FluxSource<T, T> implements Fuseable {

	final Function<? super T, ? extends K> keyExtractor;

	final Supplier<C> collectionSupplier;

	public FluxDistinctFuseable(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
							 Supplier<C> collectionSupplier) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		C collection;

		try {
			collection = collectionSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (collection == null) {
			EmptySubscription.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
			return;
		}
		
		source.subscribe(new DistinctFuseableSubscriber<>(s, collection, keyExtractor));
	}
}

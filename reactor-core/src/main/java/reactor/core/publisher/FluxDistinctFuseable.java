/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.BiPredicate;
import java.util.function.Consumer;
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
 * @param <C> the backing store type used together with the keys when testing for duplicates with {@link BiPredicate}
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDistinctFuseable<T, K, C>
		extends InternalFluxOperator<T, T> implements Fuseable {

	final Function<? super T, ? extends K> keyExtractor;
	final Supplier<C>                      collectionSupplier;
	final BiPredicate<C, K>                distinctPredicate;
	final Consumer<C>                      cleanupCallback;

	FluxDistinctFuseable(Flux<? extends T> source,
			Function<? super T, ? extends K> keyExtractor, Supplier<C> collectionSupplier,
			BiPredicate<C, K> distinctPredicate, Consumer<C> cleanupCallback) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
		this.distinctPredicate = Objects.requireNonNull(distinctPredicate, "distinctPredicate");
		this.cleanupCallback = Objects.requireNonNull(cleanupCallback, "cleanupCallback");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		C collection = Objects.requireNonNull(collectionSupplier.get(),
				"The collectionSupplier returned a null collection");

		return new DistinctFuseableSubscriber<>(actual, collection, keyExtractor,
				distinctPredicate, cleanupCallback);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

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

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoFlattenIterable<T, R> extends FluxFromMonoOperator<T, R>
		implements Fuseable {

	final Function<? super T, ? extends Iterable<? extends R>> mapper;

	final int prefetch;

	final Supplier<Queue<T>> queueSupplier;

	MonoFlattenIterable(Mono<? extends T> source,
			Function<? super T, ? extends Iterable<? extends R>> mapper,
			int prefetch,
			Supplier<Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) throws Exception {
		if (source instanceof Callable) {
			T v = ((Callable<T>) source).call();

			if (v == null) {
				Operators.complete(actual);
				return null;
			}

			Iterable<? extends R> iter = mapper.apply(v);
			Iterator<? extends R>  it = iter.iterator();
			boolean itFinite = FluxIterable.checkFinite(iter);

			FluxIterable.subscribe(actual, it, itFinite);

			return null;
		}
		return new FluxFlattenIterable.FlattenIterableSubscriber<>(actual,
				mapper,
				prefetch,
				queueSupplier);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

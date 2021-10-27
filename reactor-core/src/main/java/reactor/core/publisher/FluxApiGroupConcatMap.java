/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Function;

import org.reactivestreams.Publisher;

/**
 * A {@link Flux} API sub-group that exposes all the concatMap operators and variants. Exposed via {@link Flux#concatMaps()}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupConcatMap<T> {

	private final Flux<T> source;

	FluxApiGroupConcatMap(Flux<T> source) {
		this.source = source;
	}

	//FIXME find a better naming ? or should we use similar naming for FluxApiGroupFlatMap#interleaved ?
	public <V> Flux<V> map(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return source.concatMap(mapper);
	}

	public <V> Flux<V> map(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		return source.concatMap(mapper, prefetch);
	}

	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return source.concatMapDelayError(mapper);
	}

	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		return source.concatMapDelayError(mapper, prefetch);
	}

	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
									 boolean delayUntilEnd, int prefetch) {
		return source.concatMapDelayError(mapper, delayUntilEnd, prefetch);
	}

	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return source.concatMapIterable(mapper);
	}

	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
		return source.concatMapIterable(mapper, prefetch);
	}
}

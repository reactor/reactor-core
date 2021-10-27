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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.util.annotation.Nullable;

/**
 * A {@link Flux} API sub-group that offers all the flavors of flatMapping operators.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupFlatMap<T> {

	private final Flux<T> source;

	FluxApiGroupFlatMap(Flux<T> source) {
		this.source = source;
	}

	public <R> Flux<R> interleaved(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return source.flatMap(mapper);
	}

	public <V> Flux<V> interleaved(Function<? super T, ? extends Publisher<? extends V>> mapper, int concurrency) {
		return source.flatMap(mapper, concurrency);
	}

	public <V> Flux<V> interleaved(Function<? super T, ? extends Publisher<? extends V>> mapper, int concurrency, int prefetch) {
		return source.flatMap(mapper, concurrency, prefetch);
	}

	public <V> Flux<V> interleavedDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
									 int concurrency, int prefetch) {
		return source.flatMapDelayError(mapper, concurrency, prefetch);
	}

	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return source.flatMapIterable(mapper);
	}

	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper,
								   int prefetch) {
		return source.flatMapIterable(mapper, prefetch);
	}

	public <R> Flux<R> sequential(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return source.flatMapSequential(mapper);
	}

	public <R> Flux<R> sequential(Function<? super T, ? extends Publisher<? extends R>> mapper, int maxConcurrency) {
		return source.flatMapSequential(mapper, maxConcurrency);
	}

	public <R> Flux<R> sequential(Function<? super T, ? extends Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return source.flatMapSequential(mapper, maxConcurrency, prefetch);
	}

	public <R> Flux<R> sequentialDelayError(Function<? super T, ? extends Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return source.flatMapSequentialDelayError(mapper, maxConcurrency, prefetch);
	}

	public <R> Flux<R> signals(
		@Nullable Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
		@Nullable Function<? super Throwable, ? extends Publisher<? extends R>> mapperOnError,
		@Nullable Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return source.flatMap(mapperOnNext, mapperOnError, mapperOnComplete);
	}
}

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

import java.util.Comparator;

import org.reactivestreams.Publisher;

/**
 * A set of {@link Flux} factory methods around merging of multiple publishers.
 * Exposed through {@link Flux#fromMerging()}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiFactoryMerge {

	public static final FluxApiFactoryMerge INSTANCE = new FluxApiFactoryMerge();

	private FluxApiFactoryMerge() {
	}

	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return Flux.merge(source);
	}

	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency) {
		return Flux.merge(source, concurrency);
	}

	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency, int prefetch) {
		return Flux.merge(source, concurrency, prefetch);
	}

	public <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return Flux.merge(sources);
	}

	@SafeVarargs
	public final <I> Flux<I> merge(Publisher<? extends I>... sources) {
		return Flux.merge(sources);
	}

	@SafeVarargs
	public final <I> Flux<I> merge(int prefetch, Publisher<? extends I>... sources) {
		return Flux.merge(prefetch, sources);
	}

	@SafeVarargs
	public final <I> Flux<I> mergeDelayError(int prefetch, Publisher<? extends I>... sources) {
		return Flux.mergeDelayError(prefetch, sources);
	}

	@SafeVarargs
	public final <I extends Comparable<? super I>> Flux<I> mergeComparing(Publisher<? extends I>... sources) {
		return Flux.mergeComparing(sources);
	}

	@SafeVarargs
	public final <T> Flux<T> mergeComparing(Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return Flux.mergeComparing(comparator, sources);
	}

	@SafeVarargs
	public final <T> Flux<T> mergeComparing(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return Flux.mergeComparing(prefetch, comparator, sources);
	}

	@SafeVarargs
	public final <T> Flux<T> mergeComparingDelayError(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return Flux.mergeComparingDelayError(prefetch, comparator, sources);
	}

	public <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources) {
		return Flux.mergeSequential(sources);
	}

	public <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources,
											  int maxConcurrency, int prefetch) {
		return Flux.mergeSequential(sources, maxConcurrency, prefetch);
	}

	public <T> Flux<T> mergeSequentialDelayError(Publisher<? extends Publisher<? extends T>> sources,
														int maxConcurrency, int prefetch) {
		return Flux.mergeSequentialDelayError(sources, maxConcurrency, prefetch);
	}

	@SafeVarargs
	public final <I> Flux<I> mergeSequential(Publisher<? extends I>... sources) {
		return Flux.mergeSequential(sources);
	}

	@SafeVarargs
	public final <I> Flux<I> mergeSequential(int prefetch, Publisher<? extends I>... sources) {
		return Flux.mergeSequential(prefetch, sources);
	}

	@SafeVarargs
	public final <I> Flux<I> mergeSequentialDelayError(int prefetch, Publisher<? extends I>... sources) {
		return Flux.mergeSequentialDelayError(prefetch, sources);
	}

	public <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources) {
		return Flux.mergeSequential(sources);
	}

	public <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources,
											  int maxConcurrency, int prefetch) {
		return Flux.mergeSequential(sources, maxConcurrency, prefetch);
	}

	public <I> Flux<I> mergeSequentialDelayError(Iterable<? extends Publisher<? extends I>> sources,
														int maxConcurrency, int prefetch) {
		return Flux.mergeSequentialDelayError(sources, maxConcurrency, prefetch);
	}

	// TODO == private static factories that all methods delegate to ==
	// Note that we use instance method + singleton for the actual operators because we need to expose these as public, but
	// don't want users to have direct access to static methods in the present class.
}

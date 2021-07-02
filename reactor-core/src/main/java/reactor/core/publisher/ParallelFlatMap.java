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

import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Flattens the generated Publishers on each rail.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
final class ParallelFlatMap<T, R> extends ParallelFlux<R> implements Scannable{

	final ParallelFlux<T> source;
	
	final Function<? super T, ? extends Publisher<? extends R>> mapper;
	
	final boolean delayError;
	
	final int maxConcurrency;
	
	final Supplier<? extends Queue<R>> mainQueueSupplier;

	final int prefetch;
	
	final Supplier<? extends Queue<R>> innerQueueSupplier;

	ParallelFlatMap(
			ParallelFlux<T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError, 
			int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, 
			int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
		this.source = source;
		this.mapper = mapper;
		this.delayError = delayError;
		this.maxConcurrency = maxConcurrency;
		this.mainQueueSupplier = mainQueueSupplier;
		this.prefetch = prefetch;
		this.innerQueueSupplier = innerQueueSupplier;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}
	
	@Override
	public void subscribe(CoreSubscriber<? super R>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		@SuppressWarnings("unchecked")
		CoreSubscriber<T>[] parents = new CoreSubscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = new FluxFlatMap.FlatMapMain<>(subscribers[i],
					mapper,
					delayError,
					maxConcurrency,
					mainQueueSupplier,
					prefetch,
					innerQueueSupplier);
		}
		
		source.subscribe(parents);
	}
}

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

import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.*;
import reactor.core.Scannable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

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
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

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
	public void subscribe(Subscriber<? super R>[] subscribers, Context ctx) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		@SuppressWarnings("unchecked")
		Subscriber<T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = subscriber(subscribers[i], mapper, delayError,
					maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier);
		}
		
		source.subscribe(parents, ctx);
	}

	static <T, R> Subscriber<T> subscriber(Subscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError,
			int maxConcurrency,
			Supplier<? extends Queue<R>> mainQueueSupplier,
			int prefetch,
			Supplier<? extends Queue<R>> innerQueueSupplier) {
		return new FluxFlatMap.FlatMapMain<>(s,
				mapper,
				delayError,
				maxConcurrency,
				mainQueueSupplier,
				prefetch,
				innerQueueSupplier);
	}
}

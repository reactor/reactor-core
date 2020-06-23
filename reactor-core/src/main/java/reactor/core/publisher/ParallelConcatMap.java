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

import java.util.Objects;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.util.annotation.Nullable;

/**
 * Concatenates the generated Publishers on each rail.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
final class ParallelConcatMap<T, R> extends ParallelFlux<R> implements Scannable{

	final ParallelFlux<T> source;
	
	final Function<? super T, ? extends Publisher<? extends R>> mapper;
	
	final Supplier<? extends Queue<T>> queueSupplier;
	
	final int prefetch;
	
	final ErrorMode errorMode;

	ParallelConcatMap(
			ParallelFlux<T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper, 
					Supplier<? extends Queue<T>> queueSupplier,
					int prefetch, ErrorMode errorMode) {
		this.source = source;
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
		this.errorMode = Objects.requireNonNull(errorMode, "errorMode");
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.DELAY_ERROR) return errorMode != ErrorMode.IMMEDIATE;
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
			parents[i] = FluxConcatMap.subscriber(subscribers[i], mapper,
					queueSupplier, prefetch, errorMode);
		}
		
		source.subscribe(parents);
	}
}

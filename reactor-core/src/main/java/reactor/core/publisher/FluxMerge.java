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
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Merges a fixed array of Publishers.
 * @param <T> the element type of the publishers
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxMerge<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] sources;
	
	final boolean delayError;
	
	final int maxConcurrency;
	
	final Supplier<? extends Queue<T>> mainQueueSupplier;

	final int prefetch;
	
	final Supplier<? extends Queue<T>> innerQueueSupplier;
	
	FluxMerge(Publisher<? extends T>[] sources,
			boolean delayError, int maxConcurrency, 
			Supplier<? extends Queue<T>> mainQueueSupplier, 
					int prefetch, Supplier<? extends Queue<T>> innerQueueSupplier) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		if (maxConcurrency <= 0) {
			throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
		}
		this.sources = Objects.requireNonNull(sources, "sources");
		this.delayError = delayError;
		this.maxConcurrency = maxConcurrency;
		this.prefetch = prefetch;
		this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
	}
	
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		FluxFlatMap.FlatMapMain<Publisher<? extends T>, T> merger = new FluxFlatMap.FlatMapMain<>(
				actual, identityFunction(), delayError, maxConcurrency, mainQueueSupplier, prefetch,
				innerQueueSupplier);
		
		merger.onSubscribe(new FluxArray.ArraySubscription<>(merger, sources));
	}
	
	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxMerge instance.
	 * 
	 * @param source the new source to merge with the others
	 * @param newQueueSupplier a function that should return a new queue supplier based on the change in the maxConcurrency value
	 * @return the new FluxMerge instance
	 */
	FluxMerge<T> mergeAdditionalSource(Publisher<? extends T> source, IntFunction<Supplier<? extends Queue<T>>> newQueueSupplier) {
		int n = sources.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(sources, 0, newArray, 0, n);
		newArray[n] = source;
		
		// increase the maxConcurrency because if merged separately, it would have run concurrently anyway
		Supplier<? extends Queue<T>> newMainQueue;
		int mc = maxConcurrency;
		if (mc != Integer.MAX_VALUE) {
			mc++;
			newMainQueue = newQueueSupplier.apply(mc);
		} else {
			newMainQueue = mainQueueSupplier;
		}
		
		return new FluxMerge<>(newArray, delayError, mc, newMainQueue, prefetch, innerQueueSupplier);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.PREFETCH) return prefetch;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

}
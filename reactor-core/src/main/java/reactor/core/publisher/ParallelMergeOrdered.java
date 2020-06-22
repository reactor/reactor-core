/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Comparator;
import java.util.Queue;
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Merges the individual 'rails' of the source {@link ParallelFlux} assuming a total order
 * of the values, by picking the smallest available value from each rail, resulting in
 * a single regular Publisher sequence (exposed as a {@link Flux}).
 *
 * @param <T> the value type
 * @author Simon Baslé
 */
final class ParallelMergeOrdered<T> extends Flux<T> implements Scannable {

	final ParallelFlux<? extends T> source;
	final int                       prefetch;
	final Supplier<Queue<T>>        queueSupplier;
	final Comparator<? super T>     valueComparator;

	ParallelMergeOrdered(ParallelFlux<? extends T> source, int prefetch,
			Supplier<Queue<T>> queueSupplier, Comparator<? super T> valueComparator) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.source = source;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
		this.valueComparator = valueComparator;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return prefetch;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		FluxMergeOrdered.MergeOrderedMainProducer<T>
				main = new FluxMergeOrdered.MergeOrderedMainProducer<>(actual, valueComparator, prefetch, source.parallelism());
		actual.onSubscribe(main);
		source.subscribe(main.subscribers);
	}
}

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

import java.util.function.Predicate;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Filters each 'rail' of the source ParallelFlux with a predicate function.
 *
 * @param <T> the input value type
 */
final class ParallelFilter<T> extends ParallelFlux<T> implements Scannable{

	final ParallelFlux<T> source;
	
	final Predicate<? super T> predicate;
	
	ParallelFilter(ParallelFlux<T> source, Predicate<? super T> predicate) {
		this.source = source;
		this.predicate = predicate;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;

		CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];

		boolean conditional = subscribers[0] instanceof Fuseable.ConditionalSubscriber;

		for (int i = 0; i < n; i++) {
			if (conditional) {
				parents[i] = new FluxFilter.FilterConditionalSubscriber<>(
						(Fuseable.ConditionalSubscriber<T>)subscribers[i], predicate);
			}
			else {
				parents[i] = new FluxFilter.FilterSubscriber<>(subscribers[i], predicate);
			}
		}

		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

}

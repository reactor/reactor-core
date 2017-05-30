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

import java.util.function.Function;

import org.reactivestreams.Subscriber;
import reactor.core.Scannable;

/**
 * Maps each 'rail' of the source ParallelFlux with a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
final class ParallelMap<T, R> extends ParallelFlux<R> implements Scannable {

	final ParallelFlux<T> source;
	
	final Function<? super T, ? extends R> mapper;
	
	ParallelMap(ParallelFlux<T> source, Function<? super T, ? extends R> mapper) {
		this.source = source;
		this.mapper = mapper;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}

	@Override
	public void subscribe(Subscriber<? super R>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<? super T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = new FluxMap.MapSubscriber<>(subscribers[i], mapper);
		}
		
		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

}

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


import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Execute a Consumer in each 'rail' for the current element passing through.
 *
 * @param <T> the value type
 */
final class ParallelLog<T> extends ParallelFlux<T> implements Scannable {

	final ParallelFlux<T> source;

	final SignalPeek<T> log;

	ParallelLog(ParallelFlux<T> source,
			SignalPeek<T> log
	) {
		this.source = source;
		this.log = log;
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
				parents[i] = new FluxPeekFuseable.PeekConditionalSubscriber<>(
						(Fuseable.ConditionalSubscriber<T>)subscribers[i], log);
			}
			else {
				parents[i] = new FluxPeek.PeekSubscriber<>(subscribers[i], log);
			}
		}
		
		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}
}

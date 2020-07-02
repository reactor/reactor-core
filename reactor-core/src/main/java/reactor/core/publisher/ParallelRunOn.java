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

import java.util.Queue;
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
final class ParallelRunOn<T> extends ParallelFlux<T> implements Scannable{
	final ParallelFlux<? extends T> source;
	
	final Scheduler scheduler;

	final int prefetch;

	final Supplier<Queue<T>> queueSupplier;
	
	ParallelRunOn(ParallelFlux<? extends T> parent,
			Scheduler scheduler, int prefetch, Supplier<Queue<T>> queueSupplier) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.source = parent;
		this.scheduler = scheduler;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		

		CoreSubscriber<T>[] parents = new CoreSubscriber[n];

		boolean conditional = subscribers[0] instanceof Fuseable.ConditionalSubscriber;

		for (int i = 0; i < n; i++) {
			Worker w = scheduler.createWorker();

			if (conditional) {
				parents[i] = new FluxPublishOn.PublishOnConditionalSubscriber<>(
						(Fuseable.ConditionalSubscriber<T>)subscribers[i],
						scheduler, w, true,
						prefetch, prefetch, queueSupplier);
			}
			else {
				parents[i] = new FluxPublishOn.PublishOnSubscriber<>(subscribers[i],
						scheduler, w, true,
						prefetch, prefetch, queueSupplier);
			}
		}
		
		source.subscribe(parents);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}
}

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
import java.util.function.Supplier;
import javax.annotation.Nullable;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
final class ParallelRunOn<T> extends ParallelFlux<T> implements Scannable, Fuseable {
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
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}
	
	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		@SuppressWarnings("unchecked")
		CoreSubscriber<T>[] parents = new CoreSubscriber[n];
		
		for (int i = 0; i < n; i++) {
			CoreSubscriber<? super T> a = subscribers[i];
			
			Worker w = scheduler.createWorker();

			CoreSubscriber<T> parent = new FluxPublishOn.PublishOnSubscriber<>(a,
					scheduler, w, true,
					prefetch, queueSupplier);
			parents[i] = parent;
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

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.*;

import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
final class ParallelRunOn<T> extends ParallelFlux<T> implements Fuseable {
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
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		@SuppressWarnings("unchecked")
		Subscriber<T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			Subscriber<? super T> a = subscribers[i];
			
			Worker w = scheduler.createWorker();

			Subscriber<T> parent = new FluxPublishOn.PublishOnSubscriber<>(a,
					scheduler, w, true,
					prefetch, queueSupplier);
			parents[i] = parent;
		}
		
		source.subscribe(parents);
	}

	@Override
	public long getPrefetch() {
		return prefetch;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}
}

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

import org.reactivestreams.Subscriber;
import reactor.core.Scannable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

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
	public void subscribe(Subscriber<? super T>[] subscribers, Context ctx) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<? super T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = new FluxPeek.PeekSubscriber<>(subscribers[i], log);
		}
		
		source.subscribe(parents, ctx);
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
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}
}

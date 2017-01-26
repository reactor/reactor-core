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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Wraps multiple Publishers into a ParallelFlux which runs them
 * in parallel.
 * 
 * @param <T> the value type
 */
final class ParallelArraySource<T> extends ParallelFlux<T> {
	final Publisher<T>[] sources;
	
	public ParallelArraySource(Publisher<T>[] sources) {
		this.sources = sources;
	}
	
	@Override
	public boolean isOrdered() {
		return false;
	}
	
	@Override
	public int parallelism() {
		return sources.length;
	}
	
	@Override
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		for (int i = 0; i < n; i++) {
			sources[i].subscribe(subscribers[i]);
		}
	}
}

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

import reactor.core.flow.Fuseable;
import reactor.core.publisher.FluxTake.TakeFuseableSubscriber;

/**
 * Takes only the first N values from the source Publisher.
 * <p>
 * If N is zero, the subscriber gets completed if the source completes, signals an error or
 * signals its first value (which is not not relayed though).
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxTakeFuseable<T> extends FluxSource<T, T> implements Fuseable {

	final long n;

	public FluxTakeFuseable(Publisher<? extends T> source, long n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.n = n;
	}

	public Publisher<? extends T> source() {
		return source;
	}

	public long n() {
		return n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new TakeFuseableSubscriber<>(s, n));
	}

	@Override
	public long getCapacity() {
		return n;
	}
}

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

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Represents a never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxNever extends Flux<Object> implements SourceProducer<Object> {

	static final Publisher<Object> INSTANCE = new FluxNever();

	FluxNever() {
		// deliberately no op
	}

	@Override
	public void subscribe(CoreSubscriber<? super Object> actual) {
		actual.onSubscribe(Operators.emptySubscription());
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	/**
	 * Returns a properly parametrized instance of this never Publisher.
	 *
	 * @param <T> the value type
	 * @return a properly parametrized instance of this never Publisher
	 */
	@SuppressWarnings("unchecked")
	static <T> Flux<T> instance() {
		return (Flux<T>) INSTANCE;
	}

}

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

import reactor.core.CoreSubscriber;

/**
 * Represents an never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoNever
extends Mono<Object> implements SourceProducer<Object>  {

	static final Mono<Object> INSTANCE = new MonoNever();

	MonoNever() {
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
	static <T> Mono<T> instance() {
		return (Mono<T>) INSTANCE;
	}

}

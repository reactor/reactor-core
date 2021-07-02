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
import reactor.util.annotation.Nullable;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxEmpty extends Flux<Object>
		implements Fuseable.ScalarCallable<Object>, SourceProducer<Object> {

	private static final Flux<Object> INSTANCE = new FluxEmpty();

	private FluxEmpty() {
		// deliberately no op
	}

	@Override
	public void subscribe(CoreSubscriber<? super Object> actual) {
		Operators.complete(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	/**
	 * Returns a properly parametrized instance of this empty Publisher.
	 *
	 * @param <T> the output type
	 * @return a properly parametrized instance of this empty Publisher
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> instance() {
		return (Flux<T>) INSTANCE;
	}

	@Override
	@Nullable
	public Object call() throws Exception {
		return null; /* Scalar optimizations on empty */
	}
}

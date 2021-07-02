/*
 * Copyright (c) 2019-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxContextWrite.ContextWriteSubscriber;
import reactor.util.context.Context;

/**
 * This {@link Function} wrapper is used by operators like {@link Flux#transform(Function)}
 * to implement the context loss detection.
 *
 */
class ContextTrackingFunctionWrapper<T, V> implements Function<Publisher<T>, CorePublisher<V>> {

	static final String CONTEXT_MARKER_PREFIX = "reactor.core.context.marker.";

	final Function<? super Publisher<T>, ? extends Publisher<V>> transformer;

	final String marker;

	ContextTrackingFunctionWrapper(Function<? super Publisher<T>, ? extends Publisher<V>> transformer) {
		this(transformer, transformer.toString());
	}

	ContextTrackingFunctionWrapper(
			Function<? super Publisher<T>, ? extends Publisher<V>> transformer,
			String marker
	) {
		this.transformer = transformer;
		this.marker = marker;
	}

	@Override
	public CorePublisher<V> apply(Publisher<T> source) {
		String key = CONTEXT_MARKER_PREFIX + System.identityHashCode(source);

		// Wrap source with a logic that will check whether the key is still there and remove it
		source = Operators.<T, T>liftPublisher((p, actual) -> {
			Context ctx = actual.currentContext();

			if (!ctx.hasKey(key)) {
				throw new IllegalStateException("Context loss after applying " + marker);
			}

			Context newContext = ctx.delete(key);
			return new ContextWriteSubscriber<>(actual, newContext);
		}).apply(source);

		Publisher<V> result = transformer.apply(source);

		// It is okay to return `CorePublisher` here since `transform` will use `from()` anyways
		return new CorePublisher<V>() {
			@Override
			public void subscribe(CoreSubscriber<? super V> actual) {
				Context ctx = actual.currentContext().put(key, true);
				CoreSubscriber<V> subscriber = new ContextWriteSubscriber<>(actual, ctx);

				if (result instanceof CorePublisher) {
					((CorePublisher<V>) result).subscribe(subscriber);
				}
				else {
					result.subscribe(subscriber);
				}
			}

			@Override
			public void subscribe(Subscriber<? super V> subscriber) {
				subscribe(Operators.toCoreSubscriber(subscriber));
			}
		};
	}
}

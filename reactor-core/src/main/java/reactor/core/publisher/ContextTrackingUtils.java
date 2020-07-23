/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * @author Simon Baslé
 * @author Sergei Egorov
 */
final class ContextTrackingUtils {

	static final String CONTEXT_MARKER_PREFIX = "reactor.core.context.marker.";

	static final String generateKey(Object o) {
		return CONTEXT_MARKER_PREFIX + System.identityHashCode(o);
	}

	/**
	 * This {@link Function} wrapper is used to implement the context loss detection.
	 *
	 */
	static class FunctionWrapper<T, V> implements Function<CorePublisher<T>, CorePublisher<V>>  {

		final Function<? super Publisher<T>, ? extends Publisher<V>> transformer;

		FunctionWrapper(Function<? super Publisher<T>, ? extends Publisher<V>> transformer) {
			this.transformer = transformer;
		}

		@Override
		public CorePublisher<V> apply(CorePublisher<T> self) {
			String key = generateKey(self);

			Publisher<V> newSource = Operators.<T, T>liftPublisher((p, actual) -> {
				Context ctx = actual.currentContext();

				if (!ctx.hasKey(key)) {
					throw new IllegalStateException("Context loss after applying " + transformer);
				}

				Context newContext = ctx.delete(key);
				return new FluxContextStart.ContextStartSubscriber<>(actual, newContext);
			}).andThen(transformer).apply(self);

			// It is okay to return `CorePublisher` here since `transform` will use `from()` anyways
			return new CorePublisher<V>() {
				@Override
				public void subscribe(CoreSubscriber<? super V> actual) {
					Context ctx = actual.currentContext().put(key, true);
					CoreSubscriber<V> subscriber = new FluxContextStart.ContextStartSubscriber<>(actual, ctx);

					if (newSource instanceof CorePublisher) {
						((CorePublisher<V>) newSource).subscribe(subscriber);
					}
					else {
						newSource.subscribe(subscriber);
					}
				}

				@Override
				public void subscribe(Subscriber<? super V> subscriber) {
					subscribe(Operators.toCoreSubscriber(subscriber));
				}
			};
		}
	}

	static final <V, T> CorePublisher<V> trackContextLossForFlux(Flux<T> originalSource,
			ContextView ctxView,
			BiFunction<? super ContextView,? super Flux<T>,? extends Publisher<V>> transformer) {
		String key = generateKey(ctxView);

		Publisher<T> sourceWithLossDetection = Operators.<T, T>liftPublisher((p, actual) -> {
			Context ctx = actual.currentContext();

			if (!ctx.hasKey(key)) {
				throw new IllegalStateException("Context loss after applying " + transformer);
			}

			Context newContext = ctx.delete(key);
			return new FluxContextStart.ContextStartSubscriber<>(actual, newContext);
		}).apply(originalSource);

		final Publisher<V> transformedByUser = transformer.apply(ctxView, Flux.from(sourceWithLossDetection));

		return new FluxOperator<V, V>(Flux.from(transformedByUser)) {

			@Override
			public void subscribe(CoreSubscriber<? super V> actual) {
				Context ctx = actual.currentContext().put(key, true);
				CoreSubscriber<V> subscriber = new FluxContextStart.ContextStartSubscriber<>(actual, ctx);

				if (transformedByUser instanceof CorePublisher) {
					((CorePublisher<V>) transformedByUser).subscribe(subscriber);
				}
				else {
					transformedByUser.subscribe(subscriber);
				}
			}
		};
	}

	static final <V, T> Mono<V> trackContextLossForMono(Mono<T> originalSource, ContextView ctxView,
			BiFunction<? super ContextView,? super Mono<T>,? extends Publisher<V>> transformer) {
		String key = generateKey(ctxView);
		Context contextWithLossDetectionKey = Context.of(ctxView).put(key, true);

		Publisher<T> wrappedSource = Operators.<T, T>liftPublisher((p, actual) -> {
			Context ctx = actual.currentContext();

			if (!ctx.hasKey(key)) {
				throw new IllegalStateException("Context loss after applying " + transformer);
			}

			Context newContext = ctx.delete(key);
			return new FluxContextStart.ContextStartSubscriber<>(actual, newContext);
		}).apply(originalSource);

		Publisher<V> transformedByUser = transformer.apply(contextWithLossDetectionKey, Mono.from(wrappedSource));
		return Mono.from(transformedByUser)
		           .subscriberContext(contextWithLossDetectionKey);
	}

}

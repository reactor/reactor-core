/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

/**
 * A set of {@link Flux} factory methods around concatenation of multiple publishers.
 * Exposed through {@link Flux#fromConcatenating()}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiFactoryConcat {

	static final FluxApiFactoryConcat INSTANCE = new FluxApiFactoryConcat();

	private FluxApiFactoryConcat() {
	}

	public <T> Flux<T> fromIterable(Iterable<? extends Publisher<? extends T>> sources) {
		return Flux.concat(sources);
	}

	public <T> Flux<T> fromPublisher(Publisher<? extends Publisher<? extends T>> sources) {
		return Flux.concat(sources);
	}

	public <T> Flux<T> fromPublisher(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return Flux.concat(sources, prefetch);
	}

	@SafeVarargs
	public final <T> Flux<T> allOf(Publisher<? extends T>... sources) {
		return Flux.concat(sources);
	}

	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources) {
		return Flux.concatDelayError(sources);
	}

	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return Flux.concatDelayError(sources, prefetch);
	}

	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources,
											   boolean delayUntilEnd, int prefetch) {
		return Flux.concatDelayError(sources, delayUntilEnd, prefetch);
	}

	@SafeVarargs
	public final <T> Flux<T> allOfDelayError(Publisher<? extends T>... sources) {
		return Flux.concatDelayError(sources);
	}
}

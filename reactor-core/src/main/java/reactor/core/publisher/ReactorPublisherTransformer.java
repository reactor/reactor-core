/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Helper visitor to apply a transformation on each known {@link Publisher} type implemented by Reactor.
 *
 * @author Sergei Egorov
 */
public interface ReactorPublisherTransformer
		extends Function<Publisher<Object>, Publisher<Object>> {

	Mono<Object> transform(Mono<Object> source);

	Flux<Object> transform(Flux<Object> source);

	ParallelFlux<Object> transform(ParallelFlux<Object> source);

	Publisher<Object> transform(Publisher<Object> source);

	@Override
	default Publisher<Object> apply(Publisher<Object> publisher) {
		if (publisher instanceof Mono) {
			return transform((Mono<Object>) publisher);
		}

		if (publisher instanceof Flux) {
			return transform((Flux<Object>) publisher);
		}

		if (publisher instanceof ParallelFlux) {
			return transform((ParallelFlux<Object>) publisher);
		}

		return transform(publisher);
	}

}

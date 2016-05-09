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

import java.util.Objects;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.queue.QueueSupplier;
import reactor.core.util.PlatformDependent;

/**
 * Peeks out values that make a filter function return false.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class MonoThen<T, R> extends MonoSource<T, R> {

	final Function<? super T, ? extends Mono<? extends R>> mapper;

	public MonoThen(Publisher<? extends T> source, Function<? super T, ? extends
			Mono<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {

		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper)) {
			return;
		}

		source.subscribe(new FluxFlatMap.FlatMapMain<>(s,
				mapper,
				false,
				Integer.MAX_VALUE,
				QueueSupplier.one(), PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.one()));
	}
}

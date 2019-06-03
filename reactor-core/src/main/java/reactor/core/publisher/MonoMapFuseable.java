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

import java.util.Objects;
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 * <p>
 * This variant allows composing fuseable stages.
 * 
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoMapFuseable<T, R> extends MonoOperator<T, R>
		implements Fuseable, Fuseable.Composite {

	final Function<? super T, ? extends R> mapper;

	/**
	 * Constructs a StreamMap instance with the given source and mapper.
	 *
	 * @param source the source Publisher instance
	 * @param mapper the mapper function
	 * @throws NullPointerException if either {@code source} or {@code mapper} is null.
	 */
	MonoMapFuseable(Mono<? extends T> source, Function<? super T, ? extends R> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super R> actual) {
		if (actual instanceof ConditionalSubscriber) {
			
			ConditionalSubscriber<? super R> cs = (ConditionalSubscriber<? super R>) actual;
			source.subscribe(new FluxMapFuseable.MapFuseableConditionalSubscriber<>(cs, mapper));
			return;
		}
		source.subscribe(new FluxMapFuseable.MapFuseableSubscriber<>(actual, mapper));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <K> MonoMapFuseable<T, K> tryCompose(Object composite, Fuseable.Composite.Type type) {
		if (type == Fuseable.Composite.Type.MAP && composite instanceof Function) {
			Function<T, K> composed = mapper.andThen((Function) composite);
			return new MonoMapFuseable<>(source, composed);
		}
		return null;
	}
}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import reactor.util.context.Context;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSwitchIfEmpty<T> extends MonoOperator<T, T> {

    final Mono<? extends T> other;

	public MonoSwitchIfEmpty(Mono<? extends T> source, Mono<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		FluxSwitchIfEmpty.SwitchIfEmptySubscriber<T> parent = new
				FluxSwitchIfEmpty.SwitchIfEmptySubscriber<>(s, other, ctx);

		s.onSubscribe(parent);

		source.subscribe(parent, ctx);
	}
}

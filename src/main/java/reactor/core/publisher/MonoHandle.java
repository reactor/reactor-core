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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

import reactor.core.Fuseable;
import reactor.core.publisher.FluxHandleFuseable.HandleFuseableSubscriber;

/**
 * Maps the values of the source publisher one-on-one via a mapper function. If the result is not {code null} then the
 * {@link Mono} will complete with this value.  If the result of the function is {@code null} then the {@link Mono}
 * will complete without a value.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoHandle<T, R> extends MonoSource<T, R> {

	final BiConsumer<SynchronousSink<R>, ? super T> handler;

	public MonoHandle(Publisher<? extends T> source, BiConsumer<SynchronousSink<R>, ? super T> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Fuseable) {
			source.subscribe(new HandleFuseableSubscriber<>(s, handler));
			return;
		}
		if (s instanceof Fuseable.ConditionalSubscriber) {
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) s;
			source.subscribe(new FluxHandle.HandleConditionalSubscriber<>(cs, handler));
			return;
		}
		source.subscribe(new FluxHandle.HandleSubscriber<>(s, handler));
	}
}

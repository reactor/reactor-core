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

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

import static reactor.core.publisher.FluxTimeout.addNameToTimeoutDescription;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item generated
 * Publisher source fires an item or completes before the next item arrives from the main
 * source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoTimeout<T, U, V> extends InternalMonoOperator<T, T> {

	final Publisher<U> firstTimeout;

	final Publisher<? extends T> other;
	final String timeoutDescription; //only useful when no `other`

	@SuppressWarnings("rawtypes")
    final static Function NEVER = e -> Flux.never();

	MonoTimeout(Mono<? extends T> source,
			Publisher<U> firstTimeout,
			String timeoutDescription) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.other = null;
		this.timeoutDescription = timeoutDescription;
	}

	MonoTimeout(Mono<? extends T> source,
			Publisher<U> firstTimeout,
			Publisher<? extends T> other) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.other = Objects.requireNonNull(other, "other");
		this.timeoutDescription = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {

		CoreSubscriber<T> serial = Operators.serialize(actual);

		FluxTimeout.TimeoutMainSubscriber<T, V> main =
				new FluxTimeout.TimeoutMainSubscriber<>(serial, NEVER, other,
						addNameToTimeoutDescription(source, timeoutDescription));

		serial.onSubscribe(main);

		FluxTimeout.TimeoutTimeoutSubscriber ts =
				new FluxTimeout.TimeoutTimeoutSubscriber(main, 0L);

		main.setTimeout(ts);

		firstTimeout.subscribe(ts);

		return main;
	}
}

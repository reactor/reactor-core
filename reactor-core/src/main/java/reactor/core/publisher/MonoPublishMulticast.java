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
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.concurrent.Queues;

/**
 * Shares a sequence for the duration of a function that may transform it and
 * consume it as many times as necessary without causing multiple subscriptions
 * to the upstream.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPublishMulticast<T, R> extends MonoOperator<T, R> implements Fuseable {

	final Function<? super Mono<T>, ? extends Mono<? extends R>> transform;

	MonoPublishMulticast(Mono<? extends T> source,
			Function<? super Mono<T>, ? extends Mono<? extends R>> transform) {
		super(source);
		this.transform = Objects.requireNonNull(transform, "transform");
	}

	@Override
	public void subscribe(CoreSubscriber<? super R> actual) {

		FluxPublishMulticast.FluxPublishMulticaster<T, R> multicast =
				new FluxPublishMulticast.FluxPublishMulticaster<>(Integer.MAX_VALUE,
						Queues.one(), actual.currentContext());

		Mono<? extends R> out;

		try {
			out = Objects.requireNonNull(transform.apply(fromDirect(multicast)),
					"The transform returned a null Mono");
		}
		catch (Throwable ex) {
			Operators.error(actual, Operators.onOperatorError(ex, actual.currentContext()));
			return;
		}

		if (out instanceof Fuseable) {
			out.subscribe(new FluxPublishMulticast.CancelFuseableMulticaster<>(actual, multicast));
		}
		else {
			out.subscribe(new FluxPublishMulticast.CancelMulticaster<>(actual, multicast));
		}

		source.subscribe(multicast);
	}

}

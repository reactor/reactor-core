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
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Delays the subscription to the main source until another Publisher
 * signals a value or completes.
 *
 * @param <T> the main source value type
 * @param <U> the other source type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDelaySubscription<T, U> extends MonoOperator<T, T>
		implements Consumer<FluxDelaySubscription.DelaySubscriptionOtherSubscriber<T, U>> {

	final Publisher<U> other;

	MonoDelaySubscription(Mono<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		other.subscribe(new FluxDelaySubscription.DelaySubscriptionOtherSubscriber<>(
				actual, this));
	}
	@Override
	public void accept(FluxDelaySubscription.DelaySubscriptionOtherSubscriber<T, U> s) {
		source.subscribe(new FluxDelaySubscription.DelaySubscriptionMainSubscriber<>(s.actual, s));
	}

}

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

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSwitchIfEmpty<T> extends InternalFluxOperator<T, T> {

	final Publisher<? extends T> other;

	FluxSwitchIfEmpty(Flux<? extends T> source,
			Publisher<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		SwitchIfEmptySubscriber<T> parent = new SwitchIfEmptySubscriber<>(actual, other);

		actual.onSubscribe(parent);

		return parent;
	}

	static final class SwitchIfEmptySubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Publisher<? extends T> other;

		boolean once;

		SwitchIfEmptySubscriber(CoreSubscriber<? super T> actual,
				Publisher<? extends T> other) {
			super(actual);
			this.other = other;
		}

		@Override
		public void onNext(T t) {
			if (!once) {
				once = true;
			}

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (!once) {
				once = true;

				other.subscribe(this);
			}
			else {
				actual.onComplete();
			}
		}
	}
}

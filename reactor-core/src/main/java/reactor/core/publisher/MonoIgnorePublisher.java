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
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoIgnorePublisher<T> extends Mono<T> implements Scannable, CoreOperator<T, T> {

	final Publisher<? extends T> source;

	@Nullable
	final CoreOperator<?, T> coreOperator;

	MonoIgnorePublisher(Publisher<? extends T> source) {
		this.source = Objects.requireNonNull(source, "publisher");
		this.coreOperator = source instanceof CoreOperator ? (CoreOperator) source : null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CoreSubscriber<? super T> subscriber = subscribeOrReturn(actual);
		if (subscriber == null) {
			return;
		}
		source.subscribe(subscriber);
	}

	@Override
	public final CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		MonoIgnoreElements.IgnoreElementsSubscriber<T> subscriber = new MonoIgnoreElements.IgnoreElementsSubscriber<>(actual);

		if (coreOperator == null) {
			source.subscribe(subscriber);
			return null;
		}
		return subscriber;
	}

	@Override
	public final CoreOperator<?, ? extends T> source() {
		return coreOperator;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Scannable.Attr.PARENT) {
			return source;
		}
		return null;
	}
}

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
import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoIgnorePublisher<T> extends Mono<T> implements Scannable {

	final Publisher<? extends T> source;

	MonoIgnorePublisher(Publisher<? extends T> source) {
		this.source = Objects.requireNonNull(source, "publisher");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(new MonoIgnoreElements.IgnoreElementsSubscriber<>(actual));
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

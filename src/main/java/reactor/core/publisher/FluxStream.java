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

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Receiver;

/**
 * Emits the contents of a Stream source.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxStream<T> extends Flux<T> implements Receiver, Fuseable {

	final Stream<? extends T> stream;

	FluxStream(Stream<? extends T> iterable) {
		this.stream = Objects.requireNonNull(iterable, "stream");
	}

	@Override
	public Object upstream() {
		return stream;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Iterator<? extends T> it;

		try {
			it = Objects.requireNonNull(stream.iterator(),
			"The stream returned a null Iterator");
		}
		catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		FluxIterable.subscribe(s, it);
	}

}

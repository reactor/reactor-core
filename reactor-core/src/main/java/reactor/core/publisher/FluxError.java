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

import reactor.core.Exceptions;
import reactor.core.Fuseable;

import reactor.core.CoreSubscriber;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxError<T> extends Flux<T> implements Fuseable.ScalarCallable, SourceProducer<T> {

	final Throwable error;

	FluxError(Throwable error) {
		this.error = Objects.requireNonNull(error);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Operators.error(actual, error);
	}

	@Override
	public Object call() throws Exception {
		if(error instanceof Exception){
			throw ((Exception)error);
		}
		throw Exceptions.propagate(error);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}

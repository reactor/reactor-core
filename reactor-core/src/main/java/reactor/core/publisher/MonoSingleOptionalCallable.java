/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Emits a single item from the source wrapped into an Optional, emits
 * an empty Optional instead for empty source.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSingleOptionalCallable<T> extends Mono<Optional<T>>
		implements Callable<Optional<T>>, SourceProducer<Optional<T>> {

	final Callable<? extends T> callable;

	MonoSingleOptionalCallable(Callable<? extends T> source) {
		this.callable = Objects.requireNonNull(source, "source");
	}

	@Override
	public void subscribe(CoreSubscriber<? super Optional<T>> actual) {
		Operators.MonoInnerProducerBase<Optional<T>>
				sds = new Operators.MonoInnerProducerBase<>(actual);

		actual.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		try {
			T t = callable.call();
			sds.complete(Optional.ofNullable(t));
		}
		catch (Throwable e) {
			actual.onError(Operators.onOperatorError(e, actual.currentContext()));
		}

	}

	@Override
	public Optional<T> block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	public Optional<T> block(Duration m) {
		final T v;

		try {
			 v = callable.call();
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}

		return Optional.ofNullable(v);
	}

	@Override
	public Optional<T> call() throws Exception {
		final T v = callable.call();

		return Optional.ofNullable(v);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}

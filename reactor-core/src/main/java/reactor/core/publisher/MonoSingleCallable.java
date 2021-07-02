/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

/**
 * Expects and emits a single item from the source Callable or signals
 * NoSuchElementException for empty source.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSingleCallable<T> extends Mono<T>
		implements Callable<T>, SourceProducer<T> {

	final Callable<? extends T> callable;
	@Nullable
	final T defaultValue;

	MonoSingleCallable(Callable<? extends T> source) {
		this.callable = Objects.requireNonNull(source, "source");
		this.defaultValue = null;
	}

	MonoSingleCallable(Callable<? extends T> source, T defaultValue) {
		this.callable = Objects.requireNonNull(source, "source");
		this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Operators.MonoInnerProducerBase<T>
				sds = new Operators.MonoInnerProducerBase<>(actual);

		actual.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		try {
			T t = callable.call();
			if (t == null && defaultValue == null) {
				actual.onError(new NoSuchElementException("Source was empty"));
			}
			else if (t == null) {
				sds.complete(defaultValue);
			}
			else {
				sds.complete(t);
			}
		}
		catch (Throwable e) {
			actual.onError(Operators.onOperatorError(e, actual.currentContext()));
		}

	}

	@Override
	public T block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	public T block(Duration m) {
		final T v;

		try {
			 v = callable.call();
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}

		if (v == null && this.defaultValue == null) {
			throw new NoSuchElementException("Source was empty");
		}
		else if (v == null) {
			return this.defaultValue;
		}

		return v;
	}

	@Override
	public T call() throws Exception {
		final T v = callable.call();

		if (v == null && this.defaultValue == null) {
			throw new NoSuchElementException("Source was empty");
		}
		else if (v == null) {
			return this.defaultValue;
		}

		return v;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}

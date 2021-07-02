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
import java.util.Objects;
import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link java.util.function.Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoCallable<T> extends Mono<T>
		implements Callable<T>, Fuseable, SourceProducer<T> {

	final Callable<? extends T> callable;

	MonoCallable(Callable<? extends T> callable) {
		this.callable = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Operators.MonoSubscriber<T, T>
				sds = new Operators.MonoSubscriber<>(actual);

		actual.onSubscribe(sds);

		if (sds.isCancelled()) {
			return;
		}

		try {
			T t = callable.call();
			if (t == null) {
				sds.onComplete();
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
	@Nullable
	public T block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	@Nullable
	public T block(Duration m) {
		try {
			return callable.call();
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}
	}

	@Override
	@Nullable
	public T call() throws Exception {
		return callable.call();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}

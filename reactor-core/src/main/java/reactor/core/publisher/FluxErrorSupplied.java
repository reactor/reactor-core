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

import java.util.Objects;
import java.util.function.Supplier;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;

/**
 * Emits a generated {@link Throwable} instance to Subscribers, lazily generated via a
 * provided {@link java.util.function.Supplier}.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxErrorSupplied<T> extends Flux<T> implements Fuseable.ScalarCallable, SourceProducer<T> {

	final Supplier<? extends Throwable> errorSupplier;

	FluxErrorSupplied(Supplier<? extends Throwable> errorSupplier) {
		this.errorSupplier = Objects.requireNonNull(errorSupplier, "errorSupplier");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Throwable error = Objects.requireNonNull(errorSupplier.get(), "errorSupplier produced a null Throwable");
		Operators.error(actual, error);
	}

	@Override
	public Object call() throws Exception {
		Throwable error = Objects.requireNonNull(errorSupplier.get(), "errorSupplier produced a null Throwable");
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

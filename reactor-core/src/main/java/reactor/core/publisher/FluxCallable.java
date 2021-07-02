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

import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * For each subscriber, a Supplier is invoked and the returned value emitted.
 *
 * @param <T> the value type;
 */
final class FluxCallable<T> extends Flux<T> implements Callable<T>, Fuseable, SourceProducer<T> {

	final Callable<T> callable;

	FluxCallable(Callable<T> callable) {
		this.callable = callable;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Operators.MonoSubscriber<T, T> wrapper = new Operators.MonoSubscriber<>(actual);
		actual.onSubscribe(wrapper);

		try {
			T v = callable.call();
			if (v == null) {
				wrapper.onComplete();
			}
			else {
				wrapper.complete(v);
			}
		}
		catch (Throwable ex) {
			actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
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

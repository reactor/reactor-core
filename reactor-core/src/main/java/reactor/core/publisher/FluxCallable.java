/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * For each subscriber, a Supplier is invoked and the returned value emitted.
 *
 * @param <T> the value type;
 */
final class FluxCallable<T> extends Flux<T> implements Callable<T>, Fuseable, SourceProducer<T> {

	final Callable<@Nullable T> callable;

	FluxCallable(Callable<@Nullable T> callable) {
		this.callable = callable;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		actual.onSubscribe(new MonoCallable.MonoCallableSubscription<>(actual, callable));
	}

	@Override
	public @Nullable T call() throws Exception {
		return callable.call();
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return SourceProducer.super.scanUnsafe(key);
	}
}

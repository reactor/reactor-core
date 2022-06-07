/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Consumer;

import reactor.core.CoreSubscriber;

/**
 * Hook into the lifecycle events and signals of a {@link Mono} and execute
 * a provided callback after any of onComplete, onError and cancel events.
 * The hook is executed only once and receives the event type that triggered
 * it ({@link SignalType#ON_COMPLETE}, {@link SignalType#ON_ERROR} or
 * {@link SignalType#CANCEL}).
 * <p>
 * Note that any exception thrown by the hook are caught and bubbled up
 * using {@link Operators#onErrorDropped(Throwable, reactor.util.context.Context)}.
 *
 * @param <T> the value type
 * @author Simon Baslé
 */
final class MonoDoFinally<T> extends InternalMonoOperator<T, T> {

	final Consumer<SignalType> onFinally;

	MonoDoFinally(Mono<? extends T> source, Consumer<SignalType> onFinally) {
		super(source);
		this.onFinally = onFinally;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return FluxDoFinally.createSubscriber(actual, onFinally);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

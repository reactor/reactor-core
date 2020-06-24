/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Peek into the lifecycle events and signals of a sequence, {@link reactor.core.Fuseable}
 * version of {@link MonoDoOnEach}.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDoOnEachFuseable<T> extends InternalMonoOperator<T, T> implements Fuseable {

	final Consumer<? super Signal<T>>       onSignal;
	@Nullable
	final BiConsumer<SignalType, Context> onSubscriptionSignal;

	MonoDoOnEachFuseable(Mono<? extends T> source, Consumer<? super Signal<T>> onSignal,
			@Nullable BiConsumer<SignalType, Context> signal) {
		super(source);
		this.onSignal = Objects.requireNonNull(onSignal, "onSignal");
		onSubscriptionSignal = signal;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (onSubscriptionSignal != null) {
			onSubscriptionSignal.accept(SignalType.SUBSCRIBE, actual.currentContext());
		}
		return FluxDoOnEach.createSubscriber(actual, onSignal, this.onSubscriptionSignal, true, true);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

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
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;

/**
 * Peek into the lifecycle events and signals of a sequence
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDoOnEach<T> extends InternalMonoOperator<T, T> {

	final Consumer<? super Signal<T>> onSignal;

	MonoDoOnEach(Mono<? extends T> source, Consumer<? super Signal<T>> onSignal) {
		super(source);
		this.onSignal = Objects.requireNonNull(onSignal, "onSignal");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return FluxDoOnEach.createSubscriber(actual, onSignal, false, true);
	}
}

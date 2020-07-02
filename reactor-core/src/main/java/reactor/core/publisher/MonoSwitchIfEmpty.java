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

import reactor.core.CoreSubscriber;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSwitchIfEmpty<T> extends InternalMonoOperator<T, T> {

    final Mono<? extends T> other;

	MonoSwitchIfEmpty(Mono<? extends T> source, Mono<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		FluxSwitchIfEmpty.SwitchIfEmptySubscriber<T> parent = new
				FluxSwitchIfEmpty.SwitchIfEmptySubscriber<>(actual, other);

		actual.onSubscribe(parent);

		return parent;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

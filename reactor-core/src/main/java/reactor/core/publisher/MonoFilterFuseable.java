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
import java.util.function.Predicate;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoFilterFuseable<T> extends InternalMonoOperator<T, T>
		implements Fuseable {

	final Predicate<? super T> predicate;

	MonoFilterFuseable(Mono<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new FluxFilterFuseable.FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual, predicate);
		}
		return new FluxFilterFuseable.FilterFuseableSubscriber<>(actual, predicate);
	}
}

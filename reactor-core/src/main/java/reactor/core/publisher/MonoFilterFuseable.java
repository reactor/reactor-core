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
final class MonoFilterFuseable<T> extends MonoOperator<T, T>
		implements Fuseable, Fuseable.Composite {

	final Predicate<? super T> predicate;

	MonoFilterFuseable(Mono<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	@SuppressWarnings("unchecked")
	public <K> MonoFilterFuseable<K> tryCompose(Object composable, Composite.Type type) {
		if (type == Composite.Type.FILTER && composable instanceof Predicate) {
			Predicate<? super T> composed = predicate.and((Predicate) composable);
			return (MonoFilterFuseable) new MonoFilterFuseable<T>(source, composed);
		}
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			source.subscribe(new FluxFilterFuseable.FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual, predicate));
			return;
		}
		source.subscribe(new FluxFilterFuseable.FilterFuseableSubscriber<>(actual, predicate));
	}
}

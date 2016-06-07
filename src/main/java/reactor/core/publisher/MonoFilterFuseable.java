/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.*;

import reactor.core.flow.Fuseable;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class MonoFilterFuseable<T> extends MonoSource<T, T>
		implements Fuseable {

	final Predicate<? super T> predicate;

	public MonoFilterFuseable(Publisher<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		if (!(source instanceof Fuseable)) {
			throw new IllegalArgumentException("The source must implement the Fuseable interface for this operator to work");
		}
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	public Predicate<? super T> predicate() {
		return predicate;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new FluxFilterFuseable.FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, predicate));
			return;
		}
		source.subscribe(new FluxFilterFuseable.FilterFuseableSubscriber<>(s, predicate));
	}
}

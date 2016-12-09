/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Subscription;

/**
 * A {@link Signal} flavour that is dedicated to the {@link SignalType#ON_NEXT ON_NEXT}
 * signal type and is mutable, allowing to minimize the number of {@link Signal} instances
 * created.
 * <p>
 * Note this implementation is not serializable, and is for advanced usage. Instances
 * returned by the factory methods of {@link Signal} should usually be used instead.
 *
 * @author Simon Baslé
 */
public class MutableNextSignal<T> extends Signal<T> {

	/**
	 * Create a new {@link MutableNextSignal} that hasn't had a value set yet.
	 *
	 * @param <T> the type of the onNext elements.
	 * @return a new mutable next {@link Signal}
	 */
	public static <T> MutableNextSignal<T> undefined() {
		return new MutableNextSignal<>(null);
	}

	/**
	 * Create a new {@link MutableNextSignal} with an initial value.
	 *
	 * @param value the initial value.
	 * @param <T> the type of the onNext elements.
	 * @return a new mutable next {@link Signal}
	 */
	public static <T> MutableNextSignal<T> of(T value) {
		return new MutableNextSignal<>(value);
	}

	private T t;

	MutableNextSignal(T t) {
		this.t = t;
	}

	@Override
	public Throwable getThrowable() {
		return null;
	}

	@Override
	public Subscription getSubscription() {
		return null;
	}

	@Override
	public T get() {
		return t;
	}

	@Override
	public SignalType getType() {
		return SignalType.ON_NEXT;
	}

	/**
	 * Mutate this {@link Signal} instance with a new value as an alternative
	 * to creating a new instance. The method returns this instance so it can
	 * be used in places where a new Signal would have been created.
	 *
	 * @param newT the new onNext value
	 * @return the mutated ON_NEXT signal
	 */
	public Signal<T> mutate(T newT) {
		this.t = newT;
		return this;
	}
}
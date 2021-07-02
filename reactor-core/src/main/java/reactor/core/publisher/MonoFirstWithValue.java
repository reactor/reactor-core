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

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.Objects;

final class MonoFirstWithValue<T> extends Mono<T> implements SourceProducer<T> {

	final Mono<? extends T>[] array;

	final Iterable<? extends Mono<? extends T>> iterable;

	private MonoFirstWithValue(Mono<? extends T>[] array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	@SafeVarargs
	MonoFirstWithValue(Mono<? extends T> first, Mono<? extends T>... others) {
		Objects.requireNonNull(first, "first");
		Objects.requireNonNull(others, "others");
		@SuppressWarnings("unchecked") Mono<? extends T>[] newArray = new Mono[others.length + 1];
		newArray[0] = first;
		System.arraycopy(others, 0, newArray, 1, others.length);
		this.array = newArray;
		this.iterable = null;
	}

	MonoFirstWithValue(Iterable<? extends Mono<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	/**
	 * Returns a new instance which has the additional sources to be flattened together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current {@link MonoFirstWithValue} instance.
	 *
	 * @param others the new sources to merge with the current sources
	 *
	 * @return the new {@link MonoFirstWithValue} instance or null if new sources cannot be added (backed by an Iterable)
	 */
	@Nullable
	@SafeVarargs
	final MonoFirstWithValue<T> firstValuedAdditionalSources(Mono<? extends T>... others) {
		Objects.requireNonNull(others, "others");
		if (others.length == 0) {
			return this;
		}
		if (array == null) {
			//iterable mode, returning null to convey 2 nested operators are needed here
			return null;
		}
		int currentSize = array.length;
		int otherSize = others.length;
		@SuppressWarnings("unchecked") Mono<? extends T>[] newArray = new Mono[currentSize + otherSize];
		System.arraycopy(array, 0, newArray, 0, currentSize);
		System.arraycopy(others, 0, newArray, currentSize, otherSize);

		return new MonoFirstWithValue<>(newArray);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		Publisher<? extends T>[] a = array;
		int n;
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			try {
				it = Objects.requireNonNull(iterable.iterator(), "The iterator returned is null");
			}
			catch (Throwable e) {
				Operators.error(actual, Operators.onOperatorError(e,
						actual.currentContext()));
				return;
			}

			for (; ; ) {

				boolean b;

				try {
					b = it.hasNext();
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (!b) {
					break;
				}

				Publisher<? extends T> p;

				try {
					p = Objects.requireNonNull(it.next(),
							"The Publisher returned by the iterator is null");
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (n == a.length) {
					Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, c, 0, n);
					a = c;
				}
				a[n++] = p;
			}

		}
		else {
			n = a.length;
		}

		if (n == 0) {
			Operators.complete(actual);
			return;
		}
		if (n == 1) {
			Publisher<? extends T> p = a[0];

			if (p == null) {
				Operators.error(actual,
						Operators.onOperatorError(new NullPointerException("The single source Publisher is null"),
								actual.currentContext()));
			}
			else {
				p.subscribe(actual);
			}
			return;
		}

		FluxFirstWithValue.RaceValuesCoordinator<T> coordinator =
				new FluxFirstWithValue.RaceValuesCoordinator<>(n);

		coordinator.subscribe(a, n, actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}

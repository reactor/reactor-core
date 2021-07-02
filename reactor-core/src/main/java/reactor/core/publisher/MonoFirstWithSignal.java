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

import java.util.Iterator;
import java.util.Objects;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * actual which responds first with any signal.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoFirstWithSignal<T> extends Mono<T> implements SourceProducer<T>  {

	final Mono<? extends T>[] array;

	final Iterable<? extends Mono<? extends T>> iterable;

	@SafeVarargs
	MonoFirstWithSignal(Mono<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	MonoFirstWithSignal(Iterable<? extends Mono<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	@Nullable
	Mono<T> orAdditionalSource(Mono<? extends T> other) {
		if (array != null) {
			int n = array.length;
			@SuppressWarnings("unchecked") Mono<? extends T>[] newArray = new Mono[n + 1];
			System.arraycopy(array, 0, newArray, 0, n);
			newArray[n] = other;

			return new MonoFirstWithSignal<>(newArray);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
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

		FluxFirstWithSignal.RaceCoordinator<T> coordinator =
				new FluxFirstWithSignal.RaceCoordinator<>(n);

		coordinator.subscribe(a, n, actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

}
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.MultiReceiver;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
final class MonoThenSupply<T> extends Mono<T> implements MultiReceiver {

	final Mono<? extends T>[] array;
	final boolean             delayError;

	MonoThenSupply(boolean delayError, Mono<? extends T>[] array) {
		this.delayError = delayError;
		this.array = array;
	}

	@SuppressWarnings("unchecked")
	MonoThenSupply(boolean delayError, Publisher<?> source, Mono<? extends T> other) {
		this(delayError,
				new Mono[]{ new MonoIgnoreElements(source),
						Objects.requireNonNull(other, "other")});
	}

	@Override
	public Iterator<?> upstreams() {
		return Arrays.asList(array)
		             .iterator();
	}

	@Override
	public long upstreamCount() {
		return array.length;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Mono<? extends T>[] a = array;

		FluxConcatArray.ConcatArraySubscriber<T> parent =
				new FluxConcatArray.ConcatArraySubscriber<>(s, a);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
	}

	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current MonoMerge instance.
	 *
	 * @param source the new source to merge with the others
	 *
	 * @return the new MonoThenSupply instance
	 */
	public MonoThenSupply<T> concatAdditionalSourceLast(Mono<? extends T> source) {
		int n = array.length;
		@SuppressWarnings("unchecked") Mono<? extends T>[] newArray = new Mono[n + 1];
		System.arraycopy(array, 0, newArray, 0, n);
		newArray[n] = source;

		return new MonoThenSupply<>(delayError, newArray);
	}

}

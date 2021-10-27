/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.scheduler.Scheduler;

/**
 * A {@link Flux} API sub-group that exposes all the buffering operators. Exposed via {@link Flux#buffers()}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupBuffer<T> {

	private final Flux<T> source;

	FluxApiGroupBuffer(Flux<T> source) {
		this.source = source;
	}

	public Flux<List<T>> all() {
		return source.buffer();
	}

	public Flux<List<T>> bySize(int maxSize) {
		return source.buffer(maxSize);
	}

	public <C extends Collection<? super T>> Flux<C> bySize(int maxSize, Supplier<C> bufferSupplier) {
		return source.buffer(maxSize, bufferSupplier);
	}

	public Flux<List<T>> bySizeOrTimeout(int maxSize, Duration maxTime) {
		return source.bufferTimeout(maxSize, maxTime);
	}

	public <C extends Collection<? super T>> Flux<C> bySizeOrTimeout(int maxSize, Duration maxTime, Supplier<C> bufferSupplier) {
		return source.bufferTimeout(maxSize, maxTime, bufferSupplier);
	}

	public Flux<List<T>> bySizeOrTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return source.bufferTimeout(maxSize, maxTime, timer);
	}

	public <C extends Collection<? super T>> Flux<C> bySizeOrTimeout(int maxSize, Duration maxTime,
																	 Scheduler timer,
																	 Supplier<C> bufferSupplier) {
		return source.bufferTimeout(maxSize, maxTime, timer, bufferSupplier);
	}

	public Flux<List<T>> bySizeWithSkip(int maxSize, int skip) {
		return source.buffer(maxSize, skip);
	}

	public <C extends Collection<? super T>> Flux<C> bySizeWithSkip(int maxSize, int skip,
																	Supplier<C> bufferSupplier) {
		return source.buffer(maxSize, skip, bufferSupplier);
	}

	public Flux<List<T>> byTime(Duration bufferingTimespan) {
		return source.buffer(bufferingTimespan);
	}

	public Flux<List<T>> byTime(Duration bufferingTimespan, Scheduler timer) {
		return source.buffer(bufferingTimespan, timer);
	}

	public Flux<List<T>> byTimeWithSkip(Duration bufferingTimespan, Duration openBufferEvery) {
		return source.buffer(bufferingTimespan, openBufferEvery);
	}

	public Flux<List<T>> byTimeWithSkip(Duration bufferingTimespan, Duration openBufferEvery, Scheduler timer) {
		return source.buffer(bufferingTimespan, openBufferEvery, timer);
	}

	public Flux<List<T>> includeUntil(Predicate<? super T> predicate) {
		return source.bufferUntil(predicate);
	}

	public Flux<List<T>> includeUntil(Predicate<? super T> predicate, boolean cutBefore) {
		return source.bufferUntil(predicate, cutBefore);
	}

	public Flux<List<T>> includeUntilChanged() {
		return source.bufferUntilChanged();
	}

	public <V> Flux<List<T>> includeUntilChanged(Function<? super T, ? extends V> keySelector) {
		return source.bufferUntilChanged(keySelector);
	}

	public <V> Flux<List<T>> includeUntilChanged(Function<? super T, ? extends V> keySelector,
												 BiPredicate<? super V, ? super V> keyComparator) {
		return source.bufferUntilChanged(keySelector, keyComparator);
	}

	public Flux<List<T>> includeWhile(Predicate<? super T> predicate) {
		return source.bufferWhile(predicate);
	}

	public Flux<List<T>> splitWhen(Publisher<?> other) {
		return source.buffer(other);
	}

	public <C extends Collection<? super T>> Flux<C> splitWhen(Publisher<?> other, Supplier<C> bufferSupplier) {
		return source.buffer(other, bufferSupplier);
	}

	public <U, V> Flux<List<T>> when(Publisher<U> bucketOpening,
									 Function<? super U, ? extends Publisher<V>> closeSelector) {
		return source.bufferWhen(bucketOpening, closeSelector);
	}

	public <U, V, C extends Collection<? super T>> Flux<C> when(Publisher<U> bucketOpening,
																Function<? super U, ? extends Publisher<V>> closeSelector,
																Supplier<C> bufferSupplier) {
		return source.bufferWhen(bucketOpening, closeSelector, bufferSupplier);
	}
}

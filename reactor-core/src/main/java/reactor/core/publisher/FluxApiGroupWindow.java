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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.scheduler.Scheduler;

/**
 * A {@link Flux} API sub-group that exposes all the windowing operators. Exposed via {@link Flux#windows()}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupWindow<T> {

	private final Flux<T> source;

	FluxApiGroupWindow(Flux<T> source) {
		this.source = source;
	}

	public final Flux<Flux<T>> bySize(int maxSize) {
		return source.window(maxSize);
	}

	public final Flux<Flux<T>> bySizeOrTimeout(int maxSize, Duration maxTime) {
		return source.windowTimeout(maxSize, maxTime);
	}

	public final Flux<Flux<T>> bySizeOrTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return source.windowTimeout(maxSize, maxTime, timer);
	}

	public final Flux<Flux<T>> bySizeWithSkip(int maxSize, int skip) {
		return source.window(maxSize, skip);
	}

	public final Flux<Flux<T>> byTime(Duration windowingTimespan) {
		return source.window(windowingTimespan);
	}

	public final Flux<Flux<T>> byTime(Duration windowingTimespan, Scheduler timer) {
		return source.window(windowingTimespan, timer);
	}

	public final Flux<Flux<T>> byTimeWithSkip(Duration windowingTimespan, Duration openWindowEvery) {
		return source.window(windowingTimespan, openWindowEvery);
	}

	public final Flux<Flux<T>> byTimeWithSkip(Duration windowingTimespan, Duration openWindowEvery, Scheduler timer) {
		return source.window(windowingTimespan, openWindowEvery, timer);
	}

	public final Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger) {
		return source.windowUntil(boundaryTrigger);
	}

	public final Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger, boolean cutBefore) {
		return source.windowUntil(boundaryTrigger, cutBefore);
	}

	public final Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger, boolean cutBefore, int prefetch) {
		return source.windowUntil(boundaryTrigger, cutBefore, prefetch);
	}

	public final <V> Flux<Flux<T>> includeUntilChanged() {
		return source.windowUntilChanged();
	}

	public final <V> Flux<Flux<T>> includeUntilChanged(Function<? super T, ? super V> keySelector) {
		return source.windowUntilChanged(keySelector);
	}

	public final <V> Flux<Flux<T>> includeUntilChanged(Function<? super T, ? extends V> keySelector,
													  BiPredicate<? super V, ? super V> keyComparator) {
		return source.windowUntilChanged(keySelector, keyComparator);
	}

	public final Flux<Flux<T>> includeWhile(Predicate<T> inclusionPredicate) {
		return source.windowWhile(inclusionPredicate);
	}

	public final Flux<Flux<T>> includeWhile(Predicate<T> inclusionPredicate, int prefetch) {
		return source.windowWhile(inclusionPredicate, prefetch);
	}

	public final Flux<Flux<T>> splitWhen(Publisher<?> boundary) {
		return source.window(boundary);
	}

	public final <U, V> Flux<Flux<T>> when(Publisher<U> bucketOpening,
												 final Function<? super U, ? extends Publisher<V>> closeSelector) {
		return source.windowWhen(bucketOpening, closeSelector);
	}
}

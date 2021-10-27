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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import reactor.util.context.Context;

/**
 * This {@link Flux} API sub-group exposes a set of specialist operators that are for advanced developers only.
 * These operators generally break some Reactive Streams rule and/or assumptions common in the majority of
 * vanilla reactor operators. As such, they are considered {@link Flux#unsafe()} for most developers.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupUnsafe<T> {

	private final Flux<T> source;

	FluxApiGroupUnsafe(Flux<T> source) {
		this.source = source;
	}

	public <R> Flux<T> influenceUpstreamToDiscardUsing(final Class<R> type, final Consumer<? super R> discardHook) {
		return source.doOnDiscard(type, discardHook);
	}

	public Flux<T> influenceUpstreamToContinueOnErrors(BiConsumer<Throwable, Object> errorConsumer) {
		return source.onErrorContinue(errorConsumer);
	}

	public <E extends Throwable> Flux<T> influenceUpstreamToContinueOnErrors(Class<E> type,
																			 BiConsumer<Throwable, Object> errorConsumer) {
		return source.onErrorContinue(type, errorConsumer);
	}

	public <E extends Throwable> Flux<T> influenceUpstreamToContinueOnErrors(Predicate<E> errorPredicate,
																			 BiConsumer<Throwable, Object> errorConsumer) {
		return source.onErrorContinue(errorPredicate, errorConsumer);
	}

	/**
	 * If a {@link #influenceUpstreamToContinueOnErrors(BiConsumer)} variant has been used downstream, reverts
	 * to the default 'STOP' mode where errors are terminal events upstream. It can be
	 * used for easier scoping of the on next failure strategy or to override the
	 * inherited strategy in a sub-stream (for example in a flatMap). It has no effect if
	 * {@link #influenceUpstreamToContinueOnErrors(BiConsumer)} has not been used downstream.
	 *
	 * @return a {@link Flux} that terminates on errors, even if {@link #influenceUpstreamToContinueOnErrors(BiConsumer)}
	 * was used downstream
	 */
	public Flux<T> stopInfluencingUpstreamToContinueOnErrors() {
		return source.onErrorStop();
	}
}

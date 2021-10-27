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

import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;

/**
 * A {@link Flux} API sub-group that exposes an extra set of side effects, either similar to the side effects exposed
 * at top level (via {@link Flux#doOn()} with more configuration parameters, or acting on signals that are more
 * rarely considered by most users. Exposed via {@link Flux#doOn() someFlux.doOn()}'s {@link FluxApiGroupDoOnCommon#advanced() advanced()} method.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupDoOnAdvanced<T> {

	private final Flux<T> source;

	FluxApiGroupDoOnAdvanced(Flux<T> source) {
		this.source = source;
	}

	public <R extends Throwable> Flux<T> onError(Class<R> clazz, Consumer<? super R> onError) {
		return this.source.doOnError(clazz, onError);
	}

	public Flux<T> onError(Predicate<? super Throwable> predicate, Consumer<? super Throwable> onError) {
		return this.source.doOnError(predicate, onError);
	}

	public Flux<T> afterTerminate(Runnable afterTerminate) {
		return this.source.doAfterTerminate(afterTerminate);
	}

	public Flux<T> onCancel(Runnable onCancel) {
		return this.source.doOnCancel(onCancel);
	}

	public Flux<T> onRequest(LongConsumer onRequest) {
		return this.source.doOnRequest(onRequest);
	}

	public Flux<T> onSubscribe(Consumer<? super Subscription> onSubscribe) {
		return this.source.doOnSubscribe(onSubscribe);
	}
}

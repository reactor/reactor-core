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

/**
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupDoOnCommon<T> {

	private final Flux<T> source;

	FluxApiGroupDoOnCommon(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Offer an extra set of side effects, either similar to the side effects exposed at
	 * this level with more configuration parameters, or acting on signals that are more
	 * rarely considered by most users.
	 *
	 * @return a new side effect api group for advanced side effects / logging
	 */
	public FluxApiGroupDoOnAdvanced<T> advanced() {
		return new FluxApiGroupDoOnAdvanced<>(this.source);
	}

	public Flux<T> combinationOf(Consumer<FluxApiGroupSideEffects<T>> sideEffectsSpec) {
		FluxApiGroupSideEffects<T> sideEffects = new FluxApiGroupSideEffects<>(this.source);
		sideEffectsSpec.accept(sideEffects);
		return sideEffects.decoratedFlux();
	}

	public Flux<T> next(Consumer<? super T> onNext) {
		return this.source.doOnNext(onNext);
	}

	public Flux<T> complete(Runnable onComplete) {
		return this.source.doOnComplete(onComplete);
	}

	public Flux<T> error(Consumer<? super Throwable> onError) {
		return this.source.doOnError(onError);
	}

	public Flux<T> terminate(Runnable onTerminate) {
		return this.source.doOnTerminate(onTerminate);
	}

	public Flux<T> each(Consumer<? super Signal<T>> signalConsumer) {
		return this.source.doOnEach(signalConsumer);
	}

	public Flux<T> eachLog() {
		return this.source.log();
	}

	public Flux<T> eachLog(String category) {
		return this.source.log(category);
	}

	public Flux<T> theBeginning(Runnable doFirst) {
		return this.source.doFirst(doFirst);
	}

	public Flux<T> theEnd(Consumer<SignalType> doFinally) {
		return this.source.doFinally(doFinally);
	}
}

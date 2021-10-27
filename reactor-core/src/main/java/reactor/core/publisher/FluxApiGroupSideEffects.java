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
 * This is a special subset of both {@link FluxApiGroupDoOnCommon} and {@link FluxApiGroupDoOnAdvanced} that is
 * exposed under {@link Flux#doOn()} via {@link FluxApiGroupDoOnCommon#combinationOf(Consumer)}.
 *
 * The idea is that when defining multiple side effects this way, they can be collapsed into one or two operator instances
 * by macro-fusion, as the lambda ensures the intermediate results are not used as dedicated {@link org.reactivestreams.Publisher}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
@SuppressWarnings("deprecation")
public final class FluxApiGroupSideEffects<T> {

	//FIXME implement macro-fusion
	private Flux<T> decoratedFlux;

	FluxApiGroupSideEffects(Flux<T> source) {
		this.decoratedFlux = source;
	}

	public FluxApiGroupSideEffects<T> onNext(Consumer<? super T> onNext) {
		this.decoratedFlux = decoratedFlux.doOnNext(onNext);
		return this;
	}

	public FluxApiGroupSideEffects<T> onComplete(Runnable onComplete) {
		this.decoratedFlux = decoratedFlux.doOnComplete(onComplete);
		return this;
	}

	public FluxApiGroupSideEffects<T> onError(Consumer<? super Throwable> onError) {
		this.decoratedFlux = decoratedFlux.doOnError(onError);
		return this;
	}

	public <R extends Throwable> FluxApiGroupSideEffects<T> onError(Class<R> clazz, Consumer<? super R> onError) {
		this.decoratedFlux = decoratedFlux.doOnError(clazz, onError);
		return this;
	}

	public FluxApiGroupSideEffects<T> onError(Predicate<? super Throwable> predicate, Consumer<? super Throwable> onError) {
		this.decoratedFlux = decoratedFlux.doOnError(predicate, onError);
		return this;
	}

	public FluxApiGroupSideEffects<T> onTerminate(Runnable onTerminate) {
		this.decoratedFlux = decoratedFlux.doOnTerminate(onTerminate);
		return this;
	}

	public FluxApiGroupSideEffects<T> afterTerminate(Runnable afterTerminate) {
		this.decoratedFlux = decoratedFlux.doAfterTerminate(afterTerminate);
		return this;
	}

	public FluxApiGroupSideEffects<T> onCancel(Runnable onCancel) {
		this.decoratedFlux = decoratedFlux.doOnCancel(onCancel);
		return this;
	}

	public FluxApiGroupSideEffects<T> onRequest(LongConsumer onRequest) {
		this.decoratedFlux = decoratedFlux.doOnRequest(onRequest);
		return this;
	}

	public FluxApiGroupSideEffects<T> onSubscribe(Consumer<? super Subscription> onSubscribe) {
		this.decoratedFlux = decoratedFlux.doOnSubscribe(onSubscribe);
		return this;
	}

	Flux<T> decoratedFlux() {
		return this.decoratedFlux;
	}
}

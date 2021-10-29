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

import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * A {@link Flux} API sub-group that exposes side effect operators to react to signals like onNext, onComplete, onError, etc...
 * The sub-group is exposed via {@link Flux#sideEffects(Consumer)}, passed to the {@link Consumer}, and is <strong>mutative</strong>.
 * <p>
 * Calls to side-effect methods are cumulative with previous similar calls. For instance, calling {@link #doOnNext(Consumer)} twice
 * (anywhere in the spec consumer) will combine the signal consumer of the first call c1 and of the second call c2 as
 * {@link Consumer#andThen(Consumer) c1.andThen(c2)}.
 * <p>
 * This means that cumulated side effects are properly ordered in their declared order, even for downstream-to-upstream signals
 * like {@link #doFirst}. Consider the following:
 * <pre><code>
 * flux.sideEffects(spec -> spec
 *     .doFirst(behaviorA)
 *     .doFirst(behaviorB)
 * );
 * //versus
 * flux.sideEffects(spec -> spec.doFirst(behaviorA))
 *     .sideEffects(spec -> spec.doFirst(behaviorB));
 * </code></pre>
 * In the first example, execution order is {@code behaviorA} then {@code behaviorB}. That is because a single instance of the doFirst
 * operator is actually created.
 * In the second example, execution order is inverted ( {@code behaviorA} then {@code behaviorB}). This is beacuse two instances of the
 * operator are created
 * The idea is that when defining multiple side effects this way, they can be collapsed into one or two operator instances
 * by macro-fusion, as the lambda ensures the intermediate results are not used as dedicated {@link org.reactivestreams.Publisher}.
 *
 * @author Simon Baslé
 */
//FIXME amend javadoc, ensure Flux methods point to this and not the reverse, ensure Flux javadocs are simplified and pointing to deprecation
public final class FluxApiGroupSideEffects<T> {

	@Nullable private Runnable               doFirst;
	@Nullable private Consumer<SignalType>   doFinally;
	@Nullable private Consumer<T>            doOnNext;
	@Nullable private Runnable               doOnComplete;
	@Nullable private Consumer<Throwable>    doOnError;
	@Nullable private Runnable               doAfterTerminate;
	@Nullable private Consumer<Subscription> doOnSubscribe;
	@Nullable private LongConsumer           doOnRequest;
	@Nullable private Runnable               doOnCancel;

	FluxApiGroupSideEffects() { }

	public FluxApiGroupSideEffects<T> doFirst(Runnable onFirst) {
		if (this.doFirst == null) {
			this.doFirst = onFirst;
		}
		else {
			Runnable r1 = this.doFirst;
			this.doFirst = () -> {
				r1.run();
				onFirst.run();
			};
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public FluxApiGroupSideEffects<T> doFinally(Consumer<? super SignalType> onLastSignal) {
		if (this.doFinally == null) {
			this.doFinally = (Consumer<SignalType>) onLastSignal;
		}
		else {
			this.doFinally = this.doFinally.andThen(onLastSignal);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public FluxApiGroupSideEffects<T> doOnNext(Consumer<? super T> onNext) {
		if (this.doOnNext == null) {
			this.doOnNext = (Consumer<T>) onNext;
		}
		else {
			this.doOnNext = this.doOnNext.andThen(onNext);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public FluxApiGroupSideEffects<T> doOnError(Consumer<? super Throwable> onError) {
		if (this.doOnError == null) {
			this.doOnError = (Consumer<Throwable>) onError;
		}
		else {
			this.doOnError = this.doOnError.andThen(onError);
		}
		return this;
	}

	public FluxApiGroupSideEffects<T> doOnComplete(Runnable onComplete) {
		if (this.doOnComplete == null) {
			this.doOnComplete = onComplete;
		}
		else {
			Runnable r1 = this.doOnComplete;
			Runnable r2 = onComplete;
			this.doOnComplete = () -> {
				r1.run();
				r2.run();
			};
		}
		return this;
	}

	public FluxApiGroupSideEffects<T> doOnCancel(Runnable onCancel) {
		if (this.doOnCancel == null) {
			this.doOnCancel = onCancel;
		}
		else {
			Runnable r1 = this.doOnCancel;
			Runnable r2 = onCancel;
			this.doOnCancel = () -> {
				r1.run();
				r2.run();
			};
		}
		return this;
	}

	public FluxApiGroupSideEffects<T> doOnRequest(LongConsumer onRequest) {
		if (this.doOnRequest == null) {
			this.doOnRequest = onRequest;
		}
		else {
			this.doOnRequest = this.doOnRequest.andThen(onRequest);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public FluxApiGroupSideEffects<T> doOnSubscriptionReceived(Consumer<? super Subscription> onSubscriptionReceived) {
		if (this.doOnSubscribe == null) {
			this.doOnSubscribe = (Consumer<Subscription>) onSubscriptionReceived;
		}
		else {
			this.doOnSubscribe = this.doOnSubscribe.andThen(onSubscriptionReceived);
		}
		return this;
	}

	public FluxApiGroupSideEffects<T> doAfterTerminate(Runnable doAfterTerminate) {
		if (this.doAfterTerminate == null) {
			this.doAfterTerminate = doAfterTerminate;
		}
		else {
			Runnable r1 = this.doAfterTerminate;
			Runnable r2 = doAfterTerminate;
			this.doAfterTerminate = () -> {
				r1.run();
				r2.run();
			};
		}
		return this;
	}

	/**
	 * Apply the spec to the {@link Flux} originally captured and return the decorated {@link Flux}.
	 *
	 * @return a {@link Flux} with side-effects, as instructed by this spec
	 */
	Flux<T> applyTo(Flux<T> source) {
		if (this.doFirst != null) {
			source = source.doFirst(this.doFirst);
		}

		Flux<T> peek;
		boolean doPeek = this.doOnSubscribe != null || doOnNext != null || doOnError != null || doOnComplete != null
			|| doAfterTerminate != null || doOnRequest != null || doOnCancel != null;
		if (doPeek && source instanceof Fuseable) {
			peek = new FluxPeekFuseable<>(
				source,
				this.doOnSubscribe,
				this.doOnNext,
				this.doOnError,
				this.doOnComplete,
				this.doAfterTerminate,
				this.doOnRequest,
				this.doOnCancel);
		}
		else if (doPeek) {
			peek = new FluxPeek<>(
				source,
				this.doOnSubscribe,
				this.doOnNext,
				this.doOnError,
				this.doOnComplete,
				this.doAfterTerminate,
				this.doOnRequest,
				this.doOnCancel);
		}
		else {
			peek = source;
		}

		if (doFinally != null) {
			peek = peek.doFinally(this.doFinally);
		}
		return peek;
	}

	//TODO other applications could be introduced, eg. at Subscriber level for a context-aware version
}

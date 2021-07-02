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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Execute a Consumer in each 'rail' for the current element passing through.
 *
 * @param <T> the value type
 */
final class ParallelPeek<T> extends ParallelFlux<T> implements SignalPeek<T>{

	final ParallelFlux<T> source;
	
	final Consumer<? super T> onNext;
	final Consumer<? super T> onAfterNext;
	final Consumer<? super Throwable> onError;
	final Runnable onComplete;
	final Runnable onAfterTerminated;
	final Consumer<? super Subscription> onSubscribe;
	final LongConsumer onRequest;
	final Runnable onCancel;

	ParallelPeek(ParallelFlux<T> source,
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super T> onAfterNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Runnable onAfterTerminated,
			@Nullable Consumer<? super Subscription> onSubscribe,
			@Nullable LongConsumer onRequest,
			@Nullable Runnable onCancel
	) {
		this.source = source;

		this.onNext = onNext;
		this.onAfterNext = onAfterNext;
		this.onError = onError;
		this.onComplete = onComplete;
		this.onAfterTerminated = onAfterTerminated;
		this.onSubscribe = onSubscribe;
		this.onRequest = onRequest;
		this.onCancel = onCancel;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;

		CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];

		boolean conditional = subscribers[0] instanceof Fuseable.ConditionalSubscriber;

		for (int i = 0; i < n; i++) {
			if (conditional) {
				parents[i] = new FluxPeekFuseable.PeekConditionalSubscriber<>(
						(Fuseable.ConditionalSubscriber<T>)subscribers[i], this);
			}
			else {
				parents[i] = new FluxPeek.PeekSubscriber<>(subscribers[i], this);
			}
		}
		
		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribe;
	}

	@Override
	@Nullable
	public Consumer<? super T> onNextCall() {
		return onNext;
	}

	@Override
	@Nullable
	public Consumer<? super Throwable> onErrorCall() {
		return onError;
	}

	@Override
	@Nullable
	public Runnable onCompleteCall() {
		return onComplete;
	}

	@Override
	public Runnable onAfterTerminateCall() {
		return onAfterTerminated;
	}

	@Override
	@Nullable
	public LongConsumer onRequestCall() {
		return onRequest;
	}

	@Override
	@Nullable
	public Runnable onCancelCall() {
		return onCancel;
	}

	@Override
	@Nullable
	public Consumer<? super T> onAfterNextCall() {
		return onAfterNext;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}
}

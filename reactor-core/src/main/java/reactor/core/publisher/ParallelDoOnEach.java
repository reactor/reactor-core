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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Execute a Consumer in each 'rail' for the current element passing through.
 *
 * @param <T> the value type
 */
final class ParallelDoOnEach<T> extends ParallelFlux<T> implements Scannable {

	final ParallelFlux<T> source;

	final BiConsumer<Context, ? super T>         onNext;
	final BiConsumer<Context, ? super Throwable> onError;
	final Consumer<Context>                      onComplete;

	ParallelDoOnEach(
			ParallelFlux<T> source,
			@Nullable BiConsumer<Context, ? super T> onNext,
			@Nullable BiConsumer<Context, ? super Throwable> onError,
			@Nullable Consumer<Context> onComplete
	) {
		this.source = source;

		this.onNext = onNext;
		this.onError = onError;
		this.onComplete = onComplete;
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
			CoreSubscriber<? super T> subscriber = subscribers[i];
			SignalPeek<T> signalPeek = new DoOnEachSignalPeek(subscriber.currentContext());

			if (conditional) {
				parents[i] = new FluxPeekFuseable.PeekConditionalSubscriber<>(
						(Fuseable.ConditionalSubscriber<T>) subscriber, signalPeek);
			}
			else {
				parents[i] = new FluxPeek.PeekSubscriber<>(subscriber, signalPeek);
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
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	private class DoOnEachSignalPeek implements SignalPeek<T> {

		Consumer<? super T> onNextCall;

		Consumer<? super Throwable> onErrorCall;

		Runnable onCompleteCall;

		public DoOnEachSignalPeek(Context ctx) {
			onNextCall = onNext != null ? v -> onNext.accept(ctx, v) : null;
			onErrorCall = onError != null ? e -> onError.accept(ctx, e) : null;
			onCompleteCall = onComplete != null ? () -> onComplete.accept(ctx) : null;
		}

		@Override
		public Consumer<? super Subscription> onSubscribeCall() {
			return null;
		}

		@Override
		public Consumer<? super T> onNextCall() {
			return onNextCall;
		}

		@Override
		public Consumer<? super Throwable> onErrorCall() {
			return onErrorCall;
		}

		@Override
		public Runnable onCompleteCall() {
			return onCompleteCall;
		}

		@Override
		public Runnable onAfterTerminateCall() {
			return null;
		}

		@Override
		public LongConsumer onRequestCall() {
			return null;
		}

		@Override
		public Runnable onCancelCall() {
			return null;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}
	}
}

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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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

	public ParallelPeek(ParallelFlux<T> source,
			Consumer<? super T> onNext,
			Consumer<? super T> onAfterNext,
			Consumer<? super Throwable> onError,
			Runnable onComplete,
			Runnable onAfterTerminated,
			Consumer<? super Subscription> onSubscribe,
			LongConsumer onRequest,
			Runnable onCancel
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
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<? super T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = new FluxPeek.PeekSubscriber<>(subscribers[i], this);
		}
		
		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public long getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public boolean isOrdered() {
		return source.isOrdered();
	}

	@Override
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribe;
	}

	@Override
	public Consumer<? super T> onNextCall() {
		return onNext;
	}

	@Override
	public Consumer<? super Throwable> onErrorCall() {
		return onError;
	}

	@Override
	public Runnable onCompleteCall() {
		return onComplete;
	}

	@Override
	public Runnable onAfterTerminateCall() {
		return onAfterTerminated;
	}

	@Override
	public LongConsumer onRequestCall() {
		return onRequest;
	}

	@Override
	public Runnable onCancelCall() {
		return onCancel;
	}

	@Override
	public Consumer<? super T> onAfterNextCall() {
		return onAfterNext;
	}

	@Override
	public ParallelFlux<T> upstream() {
		return source;
	}
}

/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Peeks out values that make a filter function return false.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 *
 */
final class MonoPeekFuseable<T> extends InternalMonoOperator<T, T>
		implements Fuseable, SignalPeek<T> {

	final @Nullable Consumer<? super Subscription> onSubscribeCall;

	final @Nullable Consumer<? super T> onNextCall;

	final @Nullable LongConsumer onRequestCall;

	final @Nullable Runnable onCancelCall;

	MonoPeekFuseable(Mono<? extends T> source,
			@Nullable Consumer<? super Subscription> onSubscribeCall,
			@Nullable Consumer<? super T> onNextCall,
			@Nullable LongConsumer onRequestCall,
			@Nullable Runnable onCancelCall) {
		super(source);

		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new FluxPeekFuseable.PeekFuseableConditionalSubscriber<>((ConditionalSubscriber<?
					super T>) actual, this);
		}
		return new FluxPeekFuseable.PeekFuseableSubscriber<>(actual, this);
	}

	@Override
	public @Nullable Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	public @Nullable Consumer<? super T> onNextCall() {
		return onNextCall;
	}

	@Override
	public @Nullable Consumer<? super Throwable> onErrorCall() {
		return null;
	}

	@Override
	public @Nullable Runnable onCompleteCall() {
		return null;
	}

	@Override
	public @Nullable Runnable onAfterTerminateCall() {
		return null;
	}

	@Override
	public @Nullable LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	public @Nullable Runnable onCancelCall() {
		return onCancelCall;
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Peeks out values that make a filter function return false.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPeek<T> extends MonoOperator<T, T> implements SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	MonoPeek(Mono<? extends T> source,
			@Nullable Consumer<? super Subscription> onSubscribeCall,
			@Nullable Consumer<? super T> onNextCall,
			@Nullable Consumer<? super Throwable> onErrorCall,
			@Nullable Runnable onCompleteCall,
			@Nullable LongConsumer onRequestCall,
			@Nullable Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new PeekConditionalSubscriber<>(
					(ConditionalSubscriber<? super T>)s, this), ctx);
			return;
		}
		source.subscribe(new FluxPeek.PeekSubscriber<>(s, this), ctx);
	}

	@Override
	@Nullable
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	@Nullable
	public Consumer<? super T> onNextCall() {
		return onNextCall;
	}

	@Override
	@Nullable
	public Consumer<? super Throwable> onErrorCall() {
		return onErrorCall;
	}

	@Override
	@Nullable
	public Runnable onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	@Nullable
	public Runnable onAfterTerminateCall() {
		return null;
	}

	@Override
	@Nullable
	public LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	@Nullable
	public Runnable onCancelCall() {
		return onCancelCall;
	}

}

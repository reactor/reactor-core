/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Peeks out values that make a filter function return false.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPeek<T> extends InternalMonoOperator<T, T> implements SignalPeek<T> {

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

	Mono<T> newMacroFused(
			@Nullable Consumer<? super Subscription> subscribe,
			@Nullable Consumer<? super T> next,
			@Nullable Consumer<? super Throwable> error,
			@Nullable Runnable complete,
			@Nullable LongConsumer request,
			@Nullable Runnable cancel) {
		Consumer<? super Subscription> newSubscribe = this.onSubscribeCall;
		if (this.onSubscribeCall == null) newSubscribe = subscribe;
		else if (subscribe != null) newSubscribe = sub -> { this.onSubscribeCall.accept(sub); subscribe.accept(sub); };

		Consumer<? super T> newNext = this.onNextCall;
		if (this.onNextCall == null) newNext = next;
		else if (next != null) newNext = v -> { this.onNextCall.accept(v); next.accept(v); };

		Consumer<? super Throwable> newError = this.onErrorCall;
		if (this.onErrorCall == null) newError = error;
			//for errors, we want to always attempt to execute consecutive doOnError calls.
			//so we try-catch, setting previous error as a cause to the new one
		else if (error != null) newError = t -> {
			try {
				this.onErrorCall.accept(t);
			}
			catch (Throwable t2) {
				error.accept(Exceptions.addSuppressed(t2, t));
				return;
			}
			error.accept(t);
		};

		Runnable newComplete = this.onCompleteCall;
		if (this.onCompleteCall == null) newComplete = complete;
		else if (complete != null) newComplete = () -> { this.onCompleteCall.run(); complete.run(); };

		LongConsumer newRequest = this.onRequestCall;
		if (this.onRequestCall == null) newRequest = request;
		else if (request != null) newRequest = this.onRequestCall.andThen(request);

		Runnable newCancel = this.onCancelCall;
		if (this.onCancelCall == null) newCancel = cancel;
		else if (cancel != null) newCancel = () -> { this.onCancelCall.run(); cancel.run(); };

		return new MonoPeek<>(this.source, newSubscribe, newNext, newError, newComplete, newRequest, newCancel);
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new PeekConditionalSubscriber<>(
					(ConditionalSubscriber<? super T>) actual, this);
		}
		return new FluxPeek.PeekSubscriber<>(actual, this);
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

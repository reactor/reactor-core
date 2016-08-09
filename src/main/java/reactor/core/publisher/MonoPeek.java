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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekFuseableConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekFuseableSubscriber;

/**
 * Peeks out values that make a filter function return false.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPeek<T> extends MonoSource<T, T> implements Operators.SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	public MonoPeek(Publisher<? extends T> source, Operators.SignalPeek<T> peekHelper) {
		this(source,
				peekHelper.onSubscribeCall(),
				peekHelper.onNextCall(),
				peekHelper.onErrorCall(),
				peekHelper.onCompleteCall(),
				peekHelper.onAfterTerminateCall(),
				peekHelper.onRequestCall(),
				peekHelper.onCancelCall());
	}

	public MonoPeek(Publisher<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
			Consumer<? super T> onNextCall, Consumer<? super Throwable> onErrorCall, Runnable
			onCompleteCall,
			Runnable onAfterTerminateCall, LongConsumer onRequestCall, Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof Fuseable) {
			if (s instanceof ConditionalSubscriber) {
				source.subscribe(new PeekFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) s,
						this));
				return;
			}
			source.subscribe(new PeekFuseableSubscriber<>(s, this));
			return;
		}
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new PeekConditionalSubscriber<>(
					(ConditionalSubscriber<? super T>)s, this));
			return;
		}
		source.subscribe(new FluxPeek.PeekSubscriber<>(s, this));
	}

	@Override
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
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
		return onAfterTerminateCall;
	}

	@Override
	public LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	public Runnable onCancelCall() {
		return onCancelCall;
	}

	final static class OnTerminate<T> implements Consumer<T>, Runnable {

		final BiConsumer<? super T, Throwable> biConsumer;
		boolean value = false;

		public OnTerminate(BiConsumer<? super T, Throwable> biConsumer) {
			this.biConsumer = Objects.requireNonNull(biConsumer, "OnTerminate");
		}

		@Override
		public void run() {
			if(!value){
				biConsumer.accept(null, null);
			}
		}

		@Override
		public void accept(T t) {
			value = true;
			biConsumer.accept(t, null);
		}
	}

	final static class OnSuccess<T> implements Consumer<T>, Runnable {

		final Consumer<? super T> consumer;
		boolean value = false;

		public OnSuccess(Consumer<? super T> consumer) {
			this.consumer = Objects.requireNonNull(consumer, "OnSuccess");
		}

		@Override
		public void run() {
			if(!value){
				consumer.accept(null);
			}
		}

		@Override
		public void accept(T t) {
			value = true;
			consumer.accept(t);
		}
	}

	final static class AfterSuccess<T> implements Consumer<T>, Runnable {

		final BiConsumer<? super T, Throwable> biConsumer;
		final Consumer<Throwable> errorConsumer = e -> error = e;

		T value = null;
		Throwable error = null;

		public AfterSuccess(BiConsumer<? super T, Throwable> biConsumer) {
			this.biConsumer = Objects.requireNonNull(biConsumer, "AfterTerminate");
		}

		@Override
		public void run() {
			biConsumer.accept(value, error);
		}

		@Override
		public void accept(T t) {
			value = t;
		}
	}
}

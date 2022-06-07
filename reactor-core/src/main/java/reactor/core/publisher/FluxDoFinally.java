/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Fuseable.QueueSubscription;
import reactor.util.annotation.Nullable;

/**
 * Hook into the lifecycle events and signals of a {@link Flux} and execute
 * a provided callback after any of onComplete, onError and cancel events.
 * The hook is executed only once and receives the event type that triggered
 * it ({@link SignalType#ON_COMPLETE}, {@link SignalType#ON_ERROR} or
 * {@link SignalType#CANCEL}).
 * <p>
 * Note that any exception thrown by the hook are caught and bubbled up
 * using {@link Operators#onErrorDropped(Throwable, reactor.util.context.Context)}.
 *
 * @param <T> the value type
 * @author Simon Baslé
 */
final class FluxDoFinally<T> extends InternalFluxOperator<T, T> {

	final Consumer<SignalType> onFinally;

	@SuppressWarnings("unchecked")
	static <T> CoreSubscriber<T> createSubscriber(CoreSubscriber<? super T> s, Consumer<SignalType> onFinally) {

		if (s instanceof ConditionalSubscriber) {
			return new DoFinallyConditionalSubscriber<>((ConditionalSubscriber<? super T>) s, onFinally);
		}
		return new DoFinallySubscriber<>(s, onFinally);
	}

	FluxDoFinally(Flux<? extends T> source, Consumer<SignalType> onFinally) {
		super(source);
		this.onFinally = onFinally;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return createSubscriber(actual, onFinally);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static class DoFinallySubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final Consumer<SignalType> onFinally;

		volatile int once;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DoFinallySubscriber> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(DoFinallySubscriber.class, "once");

		Subscription s;

		DoFinallySubscriber(CoreSubscriber<? super T> actual, Consumer<SignalType> onFinally) {
			this.actual = actual;
			this.onFinally = onFinally;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED || key == Attr.CANCELLED)
				return once == 1;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				actual.onError(t);
			}
			finally {
				runFinally(SignalType.ON_ERROR);
			}
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			runFinally(SignalType.ON_COMPLETE);
		}

		@Override
		public void cancel() {
			s.cancel();
			runFinally(SignalType.CANCEL);
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		void runFinally(SignalType signalType) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				try {
					onFinally.accept(signalType);
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					Operators.onErrorDropped(ex, actual.currentContext());
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}

	static final class DoFinallyConditionalSubscriber<T> extends DoFinallySubscriber<T>
			implements ConditionalSubscriber<T> {

		DoFinallyConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Consumer<SignalType> onFinally) {
			super(actual, onFinally);
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean tryOnNext(T t) {
			return ((ConditionalSubscriber<? super T>)actual).tryOnNext(t);
		}
	}
}

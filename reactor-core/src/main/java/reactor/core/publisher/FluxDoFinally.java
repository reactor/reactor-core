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
	static <T> CoreSubscriber<T> createSubscriber(
			CoreSubscriber<? super T> s, Consumer<SignalType> onFinally,
			boolean fuseable) {

		if (fuseable) {
			if(s instanceof ConditionalSubscriber) {
				return new DoFinallyFuseableConditionalSubscriber<>(
						(ConditionalSubscriber<?	super T>) s, onFinally);
			}
			return new DoFinallyFuseableSubscriber<>(s, onFinally);
		}

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
		return createSubscriber(actual, onFinally, false);
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

		static final AtomicIntegerFieldUpdater<DoFinallySubscriber> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(DoFinallySubscriber.class, "once");

		QueueSubscription<T> qs;

		Subscription s;

		boolean syncFused;

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

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (s instanceof QueueSubscription) {
					this.qs = (QueueSubscription<T>)s;
				}

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

	static class DoFinallyFuseableSubscriber<T> extends DoFinallySubscriber<T>
		implements Fuseable, QueueSubscription<T> {

		DoFinallyFuseableSubscriber(CoreSubscriber<? super T> actual, Consumer<SignalType> onFinally) {
			super(actual, onFinally);
		}

		@Override
		public int requestFusion(int mode) {
			QueueSubscription<T> qs = this.qs;
			if (qs != null && (mode & Fuseable.THREAD_BARRIER) == 0) {
				int m = qs.requestFusion(mode);
				if (m != Fuseable.NONE) {
					syncFused = m == Fuseable.SYNC;
				}
				return m;
			}
			return Fuseable.NONE;
		}

		@Override
		public void clear() {
			if (qs != null) {
				qs.clear();
			}
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			if (qs == null) {
				return null;
			}
			T v = qs.poll();
			if (v == null && syncFused) {
				runFinally(SignalType.ON_COMPLETE);
			}
			return v;
		}

		@Override
		public int size() {
			return qs == null ? 0 : qs.size();
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

	static final class DoFinallyFuseableConditionalSubscriber<T> extends DoFinallyFuseableSubscriber<T>
		implements ConditionalSubscriber<T> {

		DoFinallyFuseableConditionalSubscriber(ConditionalSubscriber<? super T> actual,
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

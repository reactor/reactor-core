/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Fuseable.QueueSubscription;
import reactor.core.Producer;
import reactor.core.Receiver;

/**
 * Hook into the lifecycle events and signals of a {@link Flux} and execute
 * a provided callback after any of onComplete, onError and cancel events.
 * The hook is executed only once and receives the event type that triggered
 * it ({@link SignalType#ON_COMPLETE}, {@link SignalType#ON_ERROR} or
 * {@link SignalType#CANCEL}).
 * <p>
 * Note that any exception thrown by the hook are caught and bubbled up
 * using {@link Operators#onErrorDropped(Throwable)}.
 *
 * @param <T> the value type
 * @author Simon Baslé
 */
final class FluxDoFinally<T> extends FluxSource<T, T> {

	final Consumer<SignalType> onFinally;

	static <T> Subscriber<T> createSubscriber(Publisher<? extends T> source,
			Subscriber<? super T> s, Consumer<SignalType> onFinally) {
		Subscriber<T> subscriber;
		if (source instanceof Fuseable && s instanceof ConditionalSubscriber) {
			subscriber = new DoFinallyFuseableConditionalSubscriber<T>((ConditionalSubscriber<? super T>) s, onFinally);
		}
		else if (source instanceof Fuseable) {
			subscriber = new DoFinallyFuseableSubscriber<>(s, onFinally);
		}
		else if (s instanceof ConditionalSubscriber) {
			subscriber = new DoFinallyConditionalSubscriber<>((ConditionalSubscriber<? super T>) s, onFinally);
		}
		else {
			subscriber = new DoFinallySubscriber<>(s, onFinally);
		}
		return subscriber;
	}

	public FluxDoFinally(Publisher<? extends T> source, Consumer<SignalType> onFinally) {
		super(source);
		this.onFinally = onFinally;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(createSubscriber(source, s, onFinally));
	}

	static class DoFinallySubscriber<T> implements Subscriber<T>,
	                                               Receiver, Producer,
	                                               Subscription {

		final Subscriber<? super T> actual;

		final Consumer<SignalType> onFinally;

		volatile int once;

		static final AtomicIntegerFieldUpdater<DoFinallySubscriber> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(DoFinallySubscriber.class, "once");

		QueueSubscription<T> qs;

		Subscription s;

		boolean syncFused;

		DoFinallySubscriber(Subscriber<? super T> actual, Consumer<SignalType> onFinally) {
			this.actual = actual;
			this.onFinally = onFinally;
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
			actual.onError(t);
			runFinally(SignalType.ON_ERROR);
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
					Operators.onErrorDropped(ex);
				}
			}
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}

	static class DoFinallyFuseableSubscriber<T> extends DoFinallySubscriber<T>
		implements Fuseable, QueueSubscription<T> {

		public DoFinallyFuseableSubscriber(Subscriber<? super T> actual, Consumer<SignalType> onFinally) {
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

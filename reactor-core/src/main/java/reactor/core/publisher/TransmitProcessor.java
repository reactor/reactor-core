/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public final class TransmitProcessor<T>
        extends FluxProcessor<T, T>
        implements InnerOperator<T, T> {

	/**
	 * Create a new {@link TransmitProcessor} that will buffer on an internal queue in an
	 * unbounded fashion.
	 *
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <E> TransmitProcessor<E> create() {
		return new TransmitProcessor<>();
	}

	public static <E> TransmitProcessor<E> create(Disposable onTerminate) {
		return new TransmitProcessor<>(onTerminate);
	}

	volatile     Disposable                                                 onTerminate;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<TransmitProcessor, Disposable> ON_TERMINATE =
			AtomicReferenceFieldUpdater.newUpdater(TransmitProcessor.class, Disposable.class, "onTerminate");

	volatile boolean done;
	Throwable error;

	TransmitInner<? super T> actual;


	volatile     long                                      requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<TransmitProcessor> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(TransmitProcessor.class, "requested");

	volatile     int                                          once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<TransmitProcessor> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(TransmitProcessor.class, "once");

	volatile     Subscription                                                 s;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<TransmitProcessor, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(TransmitProcessor.class, Subscription.class, "s");

	public TransmitProcessor() {
		this.onTerminate = null;
	}

	public TransmitProcessor(Disposable onTerminate) {
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
	}


	@Override
	public int getBufferSize() {
		return 0;
	}

	void doTerminate() {
		actual = null;
		Disposable r = onTerminate;
		if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
			r.dispose();
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (isDisposed()) {
			s.cancel();
		} else {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0);
				if (r > 0) {
					s.request(r);
				}
			}
		}
	}

	@Override
	public int getPrefetch() {
		return 0;
	}

	@Override
	public Context currentContext() {
		TransmitInner<? super T> inner = this.actual;
		CoreSubscriber<? super T> actual = inner.actual();
		return inner != null ? actual.currentContext() : Context.empty();
	}

	@Override
	public void onNext(T t) {
		TransmitInner<? super T> a = actual;

		if (isDisposed()) {
			Operators.onNextDropped(t, currentContext());
			return;
		}

		a.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		TransmitInner<?> a = actual;

		if (isDisposed()) {
			Operators.onErrorDropped(t, currentContext());
			return;
		}

		error = t;
		done = true;

		doTerminate();

		if (a != null) {
			a.onError(t);
		}
	}

	@Override
	public void onComplete() {
		TransmitInner<?> a = actual;

		if (isDisposed()) {
			return;
		}

		done = true;

		doTerminate();

		if (a != null) {
			a.onComplete();
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");
		if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
			if (!isCancelled()) {
				this.actual = new TransmitInner<>(actual, this);

				if (done) {
					actual.onSubscribe(Operators.emptySubscription());
					if (error == null) {
						actual.onComplete();
					}
					else {
						actual.onError(error);
					}
				}
				else {
					actual.onSubscribe(this);
				}
			}
			else {
				actual.onError(new RuntimeException("Cancelled"));
			}
		}
		else {
			Operators.error(actual, new IllegalStateException("TransmitProcessor " +
					"allows only a single Subscriber"));
		}
	}

	@Override
	public void request(long n) {
		if (s == null && Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
		}
		else {
			s.request(n);
		}
	}

	@Override
	public void cancel() {
		if (Operators.terminate(S, this)) {
			return;
		}

		doTerminate();
	}

	@Override
	public boolean isDisposed() {
		return isCancelled() || isTerminated();
	}

	/**
	 * @return true if all subscribers have actually been cancelled and the processor auto shut down
	 */
	public boolean isCancelled() {
		return Operators.cancelledSubscription() == s;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	@Override
	@Nullable
	public Throwable getError() {
		return error;
	}

	@Nullable
	public CoreSubscriber<? super T> actual() {
		return actual == null ? null : actual.actual();
	}

	@Override
	public long downstreamCount() {
		return hasDownstreams() ? 1L : 0L;
	}

	@Override
	public boolean hasDownstreams() {
		return actual != null;
	}

	static final class TransmitInner<T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		final TransmitProcessor<T> parent;

		volatile boolean cancelled;

		volatile long                                      requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TransmitInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(TransmitInner.class, "requested");

		TransmitInner(CoreSubscriber<? super T> actual, TransmitProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				parent.cancel();
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.CANCELLED) return cancelled;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		void onNext(T value) {
			if (requested != 0L) {
				actual.onNext(value);
				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return;
			}
			parent.cancel();
			actual.onError(Exceptions.failWithOverflow(
					"Can't deliver value due to lack of requests"));
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}

	}
}

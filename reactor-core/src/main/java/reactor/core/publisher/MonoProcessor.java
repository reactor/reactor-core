/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@code MonoProcessor} is a {@link Mono} extension that implements stateful semantics. Multi-subscribe is allowed.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/monoprocessor.png" alt="">
 * <p>
 *
 * Once a {@link MonoProcessor} has been resolved, newer subscribers will benefit from the cached result.
 *
 * @param <O> the type of the value that will be made available
 *
 * @author Stephane Maldini
 */
public final class MonoProcessor<O> extends Mono<O>
		implements Processor<O, O>, CoreSubscriber<O>, Disposable, Subscription,
		           Scannable {

	/**
	 * Create a {@link MonoProcessor} that will eagerly request 1 on {@link #onSubscribe(Subscription)}, cache and emit
	 * the eventual result for 1 or N subscribers.
	 *
	 * @param <T> type of the expected value
	 *
	 * @return A {@link MonoProcessor}.
	 */
	public static <T> MonoProcessor<T> create() {
		return new MonoProcessor<>(null);
	}


	volatile NextInner<O>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<MonoProcessor, NextInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(MonoProcessor.class,
					NextInner[].class,
					"subscribers");

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] TERMINATED = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY_WITH_SOURCE = new NextInner[0];

	CorePublisher<? extends O> source;

	Throwable    error;
	O            value;


	volatile Subscription subscription;
	static final AtomicReferenceFieldUpdater<MonoProcessor, Subscription> UPSTREAM =
			AtomicReferenceFieldUpdater.newUpdater(MonoProcessor.class, Subscription
					.class, "subscription");

	MonoProcessor(@Nullable CorePublisher<? extends O> source) {
		this.source = source;
		SUBSCRIBERS.lazySet(this, source != null ? EMPTY_WITH_SOURCE : EMPTY);
	}

	@Override
	public final void cancel() {
		if (isTerminated()) {
			return;
		}

		Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			return;
		}

		source = null;
		if (s != null) {
			s.cancel();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void dispose() {
		Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			return;
		}

		source = null;
		if (s != null) {
			s.cancel();
		}


		NextInner<O>[] a;
		if ((a = SUBSCRIBERS.getAndSet(this, TERMINATED)) != TERMINATED) {
			Exception e = new CancellationException("Disposed");
			error = e;
			value = null;

			for (NextInner<O> as : a) {
				as.onError(e);
			}
		}
	}

	/**
	 * Block the calling thread indefinitely, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code MonoProcessor}
	 */
	@Override
	@Nullable
	public O block() {
		return block(null);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value as a {@link Duration}
	 *
	 * @return the value of this {@code MonoProcessor} or {@code null} if the timeout is reached and the {@code MonoProcessor} has
	 * not completed
	 */
	@Override
	@Nullable
	public O block(@Nullable Duration timeout) {
		try {
			if (!isPending()) {
				return peek();
			}

			connect();

			long delay;
			if (null == timeout) {
				delay = 0L;
			}
			else {
				delay = System.nanoTime() + timeout.toNanos();
			}
			for (; ; ) {
				NextInner<O>[] inners = subscribers;
				if (inners == TERMINATED) {
					if (error != null) {
						RuntimeException re = Exceptions.propagate(error);
						re = Exceptions.addSuppressed(re, new Exception("Mono#block terminated with an error"));
						throw re;
					}
					if (value == null) {
						return null;
					}
					return value;
				}
				if (timeout != null && delay < System.nanoTime()) {
					cancel();
					throw new IllegalStateException("Timeout on Mono blocking read");
				}

				Thread.sleep(1);
			}

		}
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();

			throw new IllegalStateException("Thread Interruption on Mono blocking read");
		}
	}

	/**
	 * Return the produced {@link Throwable} error if any or null
	 *
	 * @return the produced {@link Throwable} error if any or null
	 */
	@Nullable
	public final Throwable getError() {
		return isTerminated() ? error : null;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been interrupted via cancellation.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is cancelled, {@code false}
	 * otherwise.
	 */
	public boolean isCancelled() {
		return subscription == Operators.cancelledSubscription() && !isTerminated();
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been completed with an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} was completed with an error, {@code false} otherwise.
	 */
	public final boolean isError() {
		return getError() != null;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public final boolean isSuccess() {
		return isTerminated() && error == null;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been terminated by the
	 * source producer with a success or an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public final boolean isTerminated() {
		return subscribers == TERMINATED;
	}

	@Override
	public boolean isDisposed() {
		return isTerminated() || isCancelled();
	}

	@Override
	public final void onComplete() {
		onNext(null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void onError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription())
				== Operators.cancelledSubscription()) {
			Operators.onErrorDroppedMulticast(cause);
			return;
		}

		error = cause;
		value = null;
		source = null;

		for (NextInner<O> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(cause);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void onNext(@Nullable O value) {
		Subscription s;
		if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription()))
				== Operators.cancelledSubscription()) {
			if (value != null) {
				Operators.onNextDroppedMulticast(value);
			}
			return;
		}

		this.value = value;
		Publisher<? extends O> parent = source;
		source = null;

		NextInner<O>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (value == null) {
			for (NextInner<O> as : array) {
				as.onComplete();
			}
		}
		else {
			if (s != null && !(parent instanceof Mono)) {
				s.cancel();
			}

			for (NextInner<O> as : array) {
				as.complete(value);
			}
		}
	}

	@Override
	public final void onSubscribe(Subscription subscription) {
		if (Operators.setOnce(UPSTREAM, this, subscription)) {
			subscription.request(Long.MAX_VALUE);
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	/**
	 * Returns the value that completed this {@link MonoProcessor}. Returns {@code null} if the {@link MonoProcessor} has not been completed. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the {@link MonoProcessor}, or {@code null} if it has not been completed
	 *
	 * @throws RuntimeException if the {@link MonoProcessor} was completed with an error
	 */
	@Nullable
	public O peek() {
		if (!isTerminated()) {
			return null;
		}

		if (value != null) {
			return value;
		}

		if (error != null) {
			RuntimeException re = Exceptions.propagate(error);
			re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
			throw re;
		}

		return null;
	}

	@Override
	public final void request(long n) {
		Operators.validate(n);
	}

	@Override
	public void subscribe(final CoreSubscriber<? super O> actual) {
		NextInner<O> as = new NextInner<>(actual, this);
		actual.onSubscribe(as);
		if (add(as)) {
			if (as.isCancelled()) {
				remove(as);
			}
		}
		else {
			Throwable ex = error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				O v = value;
				if (v != null) {
					as.complete(v);
				}
				else {
					as.onComplete();
				}
			}
		}
	}

	void connect() {
		Publisher<? extends O> parent = source;
		if (parent != null && SUBSCRIBERS.compareAndSet(this, EMPTY_WITH_SOURCE, EMPTY)) {
			parent.subscribe(this);
		}
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		//touch guard
		boolean t = isTerminated();

		if (key == Attr.TERMINATED) return t;
		if (key == Attr.PARENT) return subscription;
		if (key == Attr.ERROR) return error;
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
		if (key == Attr.CANCELLED) return isCancelled();
		return null;
	}

	final boolean isPending() {
		return !isTerminated();
	}

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	public final long downstreamCount() {
		return subscribers.length;
	}

	/**
	 * Return true if any {@link Subscriber} is actively subscribed
	 *
	 * @return true if any {@link Subscriber} is actively subscribed
	 */
	public final boolean hasDownstreams() {
		return downstreamCount() != 0;
	}

	boolean add(NextInner<O> ps) {
		for (;;) {
			NextInner<O>[] a = subscribers;

			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			NextInner<O>[] b = new NextInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				Publisher<? extends O> parent = source;
				if (parent != null && a == EMPTY_WITH_SOURCE) {
					parent.subscribe(this);
				}
				return true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(NextInner<O> ps) {
		for (;;) {
			NextInner<O>[] a = subscribers;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			NextInner<O>[] b;

			if (n == 1) {
				b = EMPTY;
			} else {
				b = new NextInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	final static class NextInner<T> extends Operators.MonoSubscriber<T, T> {
		final MonoProcessor<T> parent;

		NextInner(CoreSubscriber<? super T> actual, MonoProcessor<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
				parent.remove(this);
			}
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (isCancelled()) {
				Operators.onOperatorError(t, currentContext());
			} else {
				actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			return super.scanUnsafe(key);
		}
	}

}

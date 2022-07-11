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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

// NextProcessor extends a deprecated class but is itself not deprecated and is here to stay, hence the following line is ok.
@SuppressWarnings("deprecation")
class NextProcessor<O> extends MonoProcessor<O> implements CoreSubscriber<O>, reactor.core.Disposable, Scannable {

	/**
	 * This boolean indicates a usage as `Mono#share()` where, for alignment with Flux#share(), the removal of all
	 * subscribers should lead to the cancellation of the upstream Subscription.
	 */
	final boolean isRefCounted;

	volatile NextInner<O>[] subscribers;

	/**
	 * Block the calling thread indefinitely, waiting for the completion of this {@link NextProcessor}. If the
	 * {@link NextProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@link NextProcessor}
	 */
	@Override
	@Nullable
	public O block() {
		return block(null);
	}

	@Override
	public boolean isDisposed() {
		return isTerminated();
	}

	//TODO reintroduce the boolean getters below once MonoProcessor is removed again
//	/**
//	 * Indicates whether this {@link NextProcessor} has been completed with an error.
//	 *
//	 * @return {@code true} if this {@link NextProcessor} was completed with an error, {@code false} otherwise.
//	 */
//	public final boolean isError() {
//		return getError() != null;
//	}
//
//	/**
//	 * Indicates whether this {@link NextProcessor} has been successfully completed a value.
//	 *
//	 * @return {@code true} if this {@link NextProcessor} is successful, {@code false} otherwise.
//	 */
//	public final boolean isSuccess() {
//		return isTerminated() && !isError();
//	}

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<NextProcessor, NextInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(NextProcessor.class, NextInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] TERMINATED = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY_WITH_SOURCE = new NextInner[0];

	volatile     Subscription                                             subscription;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<NextProcessor, Subscription> UPSTREAM =
			AtomicReferenceFieldUpdater.newUpdater(NextProcessor.class, Subscription.class, "subscription");

	@Nullable
	CorePublisher<? extends O> source;
	@Nullable
	Throwable error;
	@Nullable
	O         value;

	NextProcessor(@Nullable CorePublisher<? extends O> source) {
		this(source, false);
	}

	NextProcessor(@Nullable CorePublisher<? extends O> source, boolean isRefCounted) {
		this.source = source;
		this.isRefCounted = isRefCounted;
		SUBSCRIBERS.lazySet(this, source != null ? EMPTY_WITH_SOURCE : EMPTY);
	}

	/**
	 * For testing purpose.
	 * <p>
	 * Returns the value that completed this {@link NextProcessor}. Returns {@code null} if the {@link NextProcessor} has not been completed. If the
	 * {@link NextProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the {@link NextProcessor}, or {@code null} if it has not been completed
	 *
	 * @throws RuntimeException if the {@link NextProcessor} was completed with an error
	 */
	@Nullable
	@Override
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

	/**
		 * Block the calling thread for the specified time, waiting for the completion of this {@link NextProcessor}. If the
		 * {@link NextProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
		 *
		 * @param timeout the timeout value as a {@link Duration}
		 *
		 * @return the value of this {@link NextProcessor} or {@code null} if the timeout is reached and the {@link NextProcessor} has
		 * not completed
		 */
	@Override
	@Nullable
	public O block(@Nullable Duration timeout) {
		try {
			if (isTerminated()) {
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
				if (isTerminated()) {
					if (error != null) {
						RuntimeException re = Exceptions.propagate(error);
						re = Exceptions.addSuppressed(re, new Exception("Mono#block terminated with an error"));
						throw re;
					}
					return value;
				}
				if (timeout != null && delay < System.nanoTime()) {
					doCancel();
					throw new IllegalStateException("Timeout on Mono blocking read");
				}

				Thread.sleep(1);
			}

		}
		catch (InterruptedException ie) {
			Thread.currentThread()
				  .interrupt();

			throw new IllegalStateException("Thread Interruption on Mono blocking read");
		}
	}

	@Override
	public final void onComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") EmitResult emitResult = tryEmitValue(null);
	}

	void emitEmpty(Sinks.EmitFailureHandler failureHandler) {
		for (;;) {
			Sinks.EmitResult emitResult = tryEmitValue(null);
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = failureHandler.onEmitFailure(SignalType.ON_COMPLETE,
				emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
				case FAIL_OVERFLOW:
				case FAIL_CANCELLED:
				case FAIL_TERMINATED:
					return;
				case FAIL_NON_SERIALIZED:
					throw new EmissionException(emitResult,
						"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
					);
				default:
					throw new EmissionException(emitResult, "Unknown emitResult value");
			}
		}
	}

	@Override
	public final void onError(Throwable cause) {
		for (;;) {
			EmitResult emitResult = tryEmitError(cause);
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = EmitFailureHandler.FAIL_FAST.onEmitFailure(SignalType.ON_ERROR,
				emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
				case FAIL_OVERFLOW:
				case FAIL_CANCELLED:
					return;
				case FAIL_TERMINATED:
					Operators.onErrorDropped(cause, currentContext());
					return;
				case FAIL_NON_SERIALIZED:
					throw new EmissionException(emitResult,
						"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
					);
				default:
					throw new EmissionException(emitResult, "Unknown emitResult value");
			}
		}
	}

	@SuppressWarnings("unchecked")
	Sinks.EmitResult tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription()) == Operators.cancelledSubscription()) {
			return EmitResult.FAIL_TERMINATED;
		}

		error = cause;
		value = null;
		source = null;

		//no need to double check since UPSTREAM.getAndSet gates the completion already
		for (NextInner<O> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(cause);
		}
		return EmitResult.OK;
	}

	@Override
	public final void onNext(@Nullable O value) {
		if (value == null) {
			emitEmpty(EmitFailureHandler.FAIL_FAST);
			return;
		}

		for (;;) {
			EmitResult emitResult = tryEmitValue(value);
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = EmitFailureHandler.FAIL_FAST.onEmitFailure(SignalType.ON_NEXT,
				emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
					//we want to "discard" without rendering the sink terminated.
					// effectively NO-OP cause there's no subscriber, so no context :(
					return;
				case FAIL_OVERFLOW:
					Operators.onDiscard(value, currentContext());
					//the emitError will onErrorDropped if already terminated
					onError(Exceptions.failWithOverflow("Backpressure overflow during Sinks.Many#emitNext"));
					return;
				case FAIL_CANCELLED:
					Operators.onDiscard(value, currentContext());
					return;
				case FAIL_TERMINATED:
					Operators.onNextDropped(value, currentContext());
					return;
				case FAIL_NON_SERIALIZED:
					throw new EmissionException(emitResult,
						"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
					);
				default:
					throw new EmissionException(emitResult, "Unknown emitResult value");
			}
		}
	}

	EmitResult tryEmitValue(@Nullable O value) {
		Subscription s;
		if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription())) == Operators.cancelledSubscription()) {
			return EmitResult.FAIL_TERMINATED;
		}

		this.value = value;
		Publisher<? extends O> parent = source;
		source = null;

		@SuppressWarnings("unchecked") NextInner<O>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

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
		return EmitResult.OK;
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT)
			return subscription;
		//touch guard
		boolean t = isTerminated();

		if (key == Attr.TERMINATED) return t;
		if (key == Attr.CANCELLED) return !t && subscription == Operators.cancelledSubscription();
		if (key == Attr.ERROR) return getError();
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	@Override
	public long downstreamCount() {
		return subscribers.length;
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

	@Override
	// This method is inherited from a deprecated class and will be removed in 3.5.
	@SuppressWarnings("deprecation")
	public void cancel() {
		doCancel();
	}

	void doCancel() { //TODO compare with the cancellation in remove(), do we need both approaches?
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
	public final void onSubscribe(Subscription subscription) {
		if (Operators.setOnce(UPSTREAM, this, subscription)) {
			subscription.request(Long.MAX_VALUE);
		}
	}

	@Override
	// This method is inherited from a deprecated class and will be removed in 3.5.
	@SuppressWarnings("deprecation")
	public boolean isCancelled() {
		return subscription == Operators.cancelledSubscription() && !isTerminated();
	}

	/**
	 * Indicates whether this {@link NextProcessor} has been terminated by the
	 * source producer with a success or an error.
	 *
	 * @return {@code true} if this {@link NextProcessor} is successful, {@code false} otherwise.
	 */
	@Override
	public boolean isTerminated() {
		return subscribers == TERMINATED;
	}

	/**
	 * Return the produced {@link Throwable} error if any or null
	 *
	 * @return the produced {@link Throwable} error if any or null
	 */
	@Nullable
	@Override
	public Throwable getError() {
		return error;
	}

	boolean add(NextInner<O> ps) {
		for (; ; ) {
			NextInner<O>[] a = subscribers;

			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked") NextInner<O>[] b = new NextInner[n + 1];
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
		for (; ; ) {
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
			boolean disconnect = false;
			if (n == 1) {
				if (isRefCounted && source != null) {
					b = EMPTY_WITH_SOURCE;
					disconnect = true;
				}
				else {
					b = EMPTY;
				}
			}
			else {
				b = new NextInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				if (disconnect) {
					Subscription oldSubscription = UPSTREAM.getAndSet(this, null);
					if (oldSubscription != null) {
						oldSubscription.cancel();
					}
				}
				return;
			}
		}
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

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	void connect() {
		Publisher<? extends O> parent = source;
		if (parent != null && SUBSCRIBERS.compareAndSet(this, EMPTY_WITH_SOURCE, EMPTY)) {
			parent.subscribe(this);
		}
	}

	final static class NextInner<T> extends Operators.MonoSubscriber<T, T> {
		final NextProcessor<T> parent;

		NextInner(CoreSubscriber<? super T> actual, NextProcessor<T> parent) {
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
			if (!isCancelled()) {
				actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return super.scanUnsafe(key);
		}
	}
}

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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.context.Context;

/**
 * A simple base class for a {@link Subscriber} implementation that lets the user
 * perform a {@link #request(long)} and {@link #cancel()} on it directly. As the targeted
 * use case is to manually handle requests, the {@link #hookOnSubscribe(Subscription)} and
 * {@link #hookOnNext(Object)} hooks are expected to be implemented, but they nonetheless
 * default to an unbounded request at subscription time. If you need to define a {@link Context}
 * for this {@link BaseSubscriber}, simply override its {@link #currentContext()} method.
 * <p>
 * Override the other optional hooks {@link #hookOnComplete()},
 * {@link #hookOnError(Throwable)} and {@link #hookOnCancel()}
 * to customize the base behavior. You also have a termination hook,
 * {@link #hookFinally(SignalType)}.
 * <p>
 * Most of the time, exceptions triggered inside hooks are propagated to
 * {@link #onError(Throwable)} (unless there is a fatal exception). The class is in the
 * {@code reactor.core.publisher} package, as this subscriber is tied to a single
 * {@link org.reactivestreams.Publisher}.
 *
 * @author Simon Baslé
 */
public abstract class BaseSubscriber<T> implements CoreSubscriber<T>, Subscription,
                                                   Disposable {

	volatile Subscription subscription;

	static AtomicReferenceFieldUpdater<BaseSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(BaseSubscriber.class, Subscription.class, "subscription");

	/**
	 * Return current {@link Subscription}
	 * @return current {@link Subscription}
	 */
	protected Subscription upstream() {
		return subscription;
	}

	@Override
	public boolean isDisposed() {
		return subscription == Operators.cancelledSubscription();
	}

	/**
	 * {@link Disposable#dispose() Dispose} the {@link Subscription} by
	 * {@link Subscription#cancel() cancelling} it.
	 */
	@Override
	public void dispose() {
		cancel();
	}

	/**
	 * Hook for further processing of onSubscribe's Subscription. Implement this method
	 * to call {@link #request(long)} as an initial request. Values other than the
	 * unbounded {@code Long.MAX_VALUE} imply that you'll also call request in
	 * {@link #hookOnNext(Object)}.
	 * <p> Defaults to request unbounded Long.MAX_VALUE as in {@link #requestUnbounded()}
	 *
	 * @param subscription the subscription to optionally process
	 */
	protected void hookOnSubscribe(Subscription subscription){
		subscription.request(Long.MAX_VALUE);
	}

	/**
	 * Hook for processing of onNext values. You can call {@link #request(long)} here
	 * to further request data from the source {@link org.reactivestreams.Publisher} if
	 * the {@link #hookOnSubscribe(Subscription) initial request} wasn't unbounded.
	 * <p>Defaults to doing nothing.
	 *
	 * @param value the emitted value to process
	 */
	protected void hookOnNext(T value){
		// NO-OP
	}

	/**
	 * Optional hook for completion processing. Defaults to doing nothing.
	 */
	protected void hookOnComplete() {
		// NO-OP
	}

	/**
	 * Optional hook for error processing. Default is to call
	 * {@link Exceptions#errorCallbackNotImplemented(Throwable)}.
	 *
	 * @param throwable the error to process
	 */
	protected void hookOnError(Throwable throwable) {
		throw Exceptions.errorCallbackNotImplemented(throwable);
	}

	/**
	 * Optional hook executed when the subscription is cancelled by calling this
	 * Subscriber's {@link #cancel()} method. Defaults to doing nothing.
	 */
	protected void hookOnCancel() {
		//NO-OP
	}

	/**
	 * Optional hook executed after any of the termination events (onError, onComplete,
	 * cancel). The hook is executed in addition to and after {@link #hookOnError(Throwable)},
	 * {@link #hookOnComplete()} and {@link #hookOnCancel()} hooks, even if these callbacks
	 * fail. Defaults to doing nothing. A failure of the callback will be caught by
	 * {@link Operators#onErrorDropped(Throwable, reactor.util.context.Context)}.
	 *
	 * @param type the type of termination event that triggered the hook
	 * ({@link SignalType#ON_ERROR}, {@link SignalType#ON_COMPLETE} or
	 * {@link SignalType#CANCEL})
	 */
	protected void hookFinally(SignalType type) {
		//NO-OP
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (Operators.setOnce(S, this, s)) {
			try {
				hookOnSubscribe(s);
			}
			catch (Throwable throwable) {
				onError(Operators.onOperatorError(s, throwable, currentContext()));
			}
		}
	}

	@Override
	public final void onNext(T value) {
		Objects.requireNonNull(value, "onNext");
		try {
			hookOnNext(value);
		}
		catch (Throwable throwable) {
			onError(Operators.onOperatorError(subscription, throwable, value, currentContext()));
		}
	}

	@Override
	public final void onError(Throwable t) {
		Objects.requireNonNull(t, "onError");

		if (S.getAndSet(this, Operators.cancelledSubscription()) == Operators
				.cancelledSubscription()) {
			//already cancelled concurrently
			Operators.onErrorDropped(t, currentContext());
			return;
		}


		try {
			hookOnError(t);
		}
		catch (Throwable e) {
			e = Exceptions.addSuppressed(e, t);
			Operators.onErrorDropped(e, currentContext());
		}
		finally {
			safeHookFinally(SignalType.ON_ERROR);
		}
	}

	@Override
	public final void onComplete() {
		if (S.getAndSet(this, Operators.cancelledSubscription()) != Operators
				.cancelledSubscription()) {
			//we're sure it has not been concurrently cancelled
			try {
				hookOnComplete();
			}
			catch (Throwable throwable) {
				//onError itself will short-circuit due to the CancelledSubscription being set above
				hookOnError(Operators.onOperatorError(throwable, currentContext()));
			}
			finally {
				safeHookFinally(SignalType.ON_COMPLETE);
			}
		}
	}

	@Override
	public final void request(long n) {
		if (Operators.validate(n)) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
			}
		}
	}

	/**
	 * {@link #request(long) Request} an unbounded amount.
	 */
	public final void requestUnbounded() {
		request(Long.MAX_VALUE);
	}

	@Override
	public final void cancel() {
		if (Operators.terminate(S, this)) {
			try {
				hookOnCancel();
			}
			catch (Throwable throwable) {
				hookOnError(Operators.onOperatorError(subscription, throwable, currentContext()));
			}
			finally {
				safeHookFinally(SignalType.CANCEL);
			}
		}
	}

	void safeHookFinally(SignalType type) {
		try {
			hookFinally(type);
		}
		catch (Throwable finallyFailure) {
			Operators.onErrorDropped(finallyFailure, currentContext());
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}

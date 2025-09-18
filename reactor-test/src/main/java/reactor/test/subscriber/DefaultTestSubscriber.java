/*
 * Copyright (c) 2021-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.subscriber;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.util.context.Context;

/**
 * Base version of a {@link TestSubscriber} (aka non-conditional).
 *
 * @author Simon Baslé
 */
class DefaultTestSubscriber<T> implements TestSubscriber<T> {

	final long                                    initialRequest;
	final Context                                 context;
	final DefaultTestSubscriber.FusionRequirement fusionRequirement;
	final int                                     requestedFusionMode;
	final int                                     expectedFusionMode;

	@SuppressWarnings("NotNullFieldNotInitialized") // s is set in onSubscribe
	Subscription s;
	Fuseable.@Nullable QueueSubscription<T> qs;
	int fusionMode = -1;

	// state tracking
	final AtomicBoolean   cancelled;
	final List<T>         receivedOnNext;
	final List<T>         receivedPostCancellation;
	final List<Signal<T>> protocolErrors;

	final CountDownLatch                  doneLatch;
	final AtomicReference<@Nullable AssertionError> subscriptionFailure;

	volatile @Nullable Signal<T> terminalSignal;

	volatile     int                                              state;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<DefaultTestSubscriber> STATE =
			AtomicIntegerFieldUpdater.newUpdater(DefaultTestSubscriber.class, "state");

	volatile     long                                          requestedTotal;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<DefaultTestSubscriber> REQUESTED_TOTAL =
			AtomicLongFieldUpdater.newUpdater(DefaultTestSubscriber.class, "requestedTotal");

	volatile     long                                          requestedPreSubscription;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<DefaultTestSubscriber> REQUESTED_PRE_SUBSCRIPTION =
			AtomicLongFieldUpdater.newUpdater(DefaultTestSubscriber.class, "requestedPreSubscription");


	DefaultTestSubscriber(TestSubscriberBuilder options) {
		this.initialRequest = options.initialRequest;
		this.context = options.context;
		this.fusionRequirement = options.fusionRequirement;
		this.requestedFusionMode = options.requestedFusionMode;
		this.expectedFusionMode = options.expectedFusionMode;

		this.cancelled = new AtomicBoolean();
		this.receivedOnNext = new CopyOnWriteArrayList<>();
		this.receivedPostCancellation = new CopyOnWriteArrayList<>();
		this.protocolErrors = new CopyOnWriteArrayList<>();
		this.state = 0;

		this.doneLatch = new CountDownLatch(1);
		this.subscriptionFailure = new AtomicReference<>();
		REQUESTED_PRE_SUBSCRIPTION.lazySet(this, initialRequest);
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	void internalCancel() {
		Subscription s = this.s;
		if (cancelled.compareAndSet(false, true) && s != null) {
			s.cancel();
			safeClearQueue(s);
		}
	}

	void safeClearQueue(@Nullable Subscription s) {
		if (s instanceof Fuseable.QueueSubscription) {
			((Fuseable.QueueSubscription) s).clear();
		}
	}

	void subscriptionFail(String message) {
		if (this.subscriptionFailure.compareAndSet(null, new AssertionError(message))) {
			internalCancel();
			notifyDone();
		}
	}

	final void notifyDone() {
		doneLatch.countDown();
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (cancelled.get()) {
			s.cancel();
			safeClearQueue(s);
			return;
		}
		if (!Operators.validate(this.s, s)) {
			safeClearQueue(s);
			//s is already cancelled at that point, subscriptionFail will cancel this.s
			subscriptionFail("TestSubscriber must not be reused, but Subscription has already been set.");
			return;
		}
		this.s = s;
		this.fusionMode = -1;
		if (s instanceof Fuseable.QueueSubscription) {
			if (fusionRequirement == FusionRequirement.NOT_FUSEABLE) {
				subscriptionFail("TestSubscriber configured to reject QueueSubscription, got " + s);
				return;
			}

			@SuppressWarnings("unchecked") //intermediate variable to suppress via annotation for compiler's benefit
			Fuseable.QueueSubscription<T> converted = (Fuseable.QueueSubscription<T>) s;
			this.qs = converted;
			int negotiatedMode = qs.requestFusion(this.requestedFusionMode);

			if (expectedFusionMode != negotiatedMode && expectedFusionMode != Fuseable.ANY) {
				subscriptionFail("TestSubscriber negotiated fusion mode inconsistent, expected " +
						Fuseable.fusionModeName(expectedFusionMode) + " got " + Fuseable.fusionModeName(negotiatedMode));
				return;
			}
			this.fusionMode = negotiatedMode;
			if (negotiatedMode == Fuseable.SYNC) {
				for (;;) {
					if (cancelled.get()) {
						safeClearQueue(qs);
						break;
					}
					T v = qs.poll();
					if (v == null) {
						onComplete();
						break;
					}
					onNext(v);
				}
			}
			else {
				long rPre = REQUESTED_PRE_SUBSCRIPTION.getAndSet(this, -1L);
				if (rPre > 0L) {
					upstreamRequest(s, rPre);
				}
			}
		}
		else if (fusionRequirement == FusionRequirement.FUSEABLE) {
			subscriptionFail("TestSubscriber configured to require QueueSubscription, got " + s);
		}
		else if (this.initialRequest > 0L) {
			long rPre = REQUESTED_PRE_SUBSCRIPTION.getAndSet(this, -1L);
			if (rPre > 0L) {
				upstreamRequest(s, rPre);
			}
		}
	}

	@Override
	public void onNext(@Nullable T t) {
		int previousState = markOnNextStart();
		boolean wasTerminated = isMarkedTerminated(previousState);
		boolean wasOnNext = isMarkedOnNext(previousState);
		if (wasTerminated || wasOnNext) {
			//at this point, we know we haven't switched the markedOnNext bit. if it is set, let the other onNext unset it
			if (t != null) {
				this.protocolErrors.add(Signal.next(t));
			}
			else if (wasTerminated) {
				this.protocolErrors.add(Signal.error(
						new AssertionError("onNext(null) received despite SYNC fusion (which has already completed)")
				));
			}
			else {
				//due to the looping nature of SYNC fusion in onSubscribe, this shouldn't happen
				this.protocolErrors.add(Signal.error(
						new AssertionError("onNext(null) received despite SYNC fusion (with concurrent onNext)")
				));
			}
			return;
		}

		if (t == null) {
			if (this.fusionMode == Fuseable.ASYNC) {
				drainAsync(false);
				return;
			}
			else {
				subscriptionFail("onNext(null) received while ASYNC fusion not established");
			}
		}

		this.receivedOnNext.add(t);
		if (cancelled.get()) {
			this.receivedPostCancellation.add(t);
		}

		checkTerminatedAfterOnNext();
	}

	@Override
	public void onComplete() {
		Signal<T> sig = Signal.complete();

		int previousState = markTerminated();

		if (isMarkedTerminated(previousState) || isMarkedTerminating(previousState)) {
			this.protocolErrors.add(sig);
			return;
		}
		if (isMarkedOnNext(previousState)) {
			this.protocolErrors.add(sig);
			this.terminalSignal = sig;
			return; //isTerminating will be detected later, triggering the notifyDone()
		}

		this.terminalSignal = sig;

		if (fusionMode == Fuseable.ASYNC) {
			drainAsync(true);
			return;
		}

		notifyDone();
	}

	@Override
	public void onError(Throwable t) {
		Signal<T> sig = Signal.error(t);

		int previousState = markTerminated();

		if (isMarkedTerminated(previousState) || isMarkedTerminating(previousState)) {
			this.protocolErrors.add(sig);
			return;
		}
		if (isMarkedOnNext(previousState)) {
			this.protocolErrors.add(sig);
			this.terminalSignal = sig;
			return; //isTerminating will be detected later, triggering the notifyDone()
		}

		this.terminalSignal = sig;

		if (fusionMode == Fuseable.ASYNC) {
			drainAsync(true);
			return;
		}

		notifyDone();
	}

	/**
	 * Drain the subscriber in asynchronous fusion mode (assumes there is a this.qs).
	 *
	 * @param isTerminal is the draining happening from onComplete/onError?
	 */
	void drainAsync(boolean isTerminal) {
		assert this.qs != null;

		//onComplete and onError move to terminated/terminating and call drainAsync ONLY if no work in progress
		int previousState = this.state;
		if (isTerminal && isMarkedOnNext(previousState)) {
			return;
		}

		if (isMarkedTerminated(previousState)) {
			safeClearQueue(qs);
			notifyDone();
			return;
		}

		T t;

		for (; ; ) {
			if (cancelled.get()) {
				safeClearQueue(qs);
				notifyDone();
				return;
			}

			long r = REQUESTED_TOTAL.get(this);
			if (r != Long.MAX_VALUE && r - this.receivedOnNext.size() < 1) {
				//no more request for data, until next request (or termination)
				if (checkTerminatedAfterOnNext()) {
					safeClearQueue(qs);
				}
				return;
			}

			t = qs.poll();
			if (t == null) {
				//no more available data, until next request (or termination)
				if (checkTerminatedAfterOnNext()) {
					safeClearQueue(qs);
				}
				return;
			}
			this.receivedOnNext.add(t);
		}
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return terminalSignal != null || subscriptionFailure.get() != null;
		if (key == Attr.CANCELLED) return cancelled.get();
		if (key == Attr.ERROR) {
			Throwable subFailure = subscriptionFailure.get();
			Signal<T> sig = terminalSignal;
			if (sig != null && sig.getThrowable() != null) {
				return sig.getThrowable();
			}
			else return subFailure; //simplified: ok to return null if subscriptionFailure holds null
		}
		if (key == Attr.PARENT) return s;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return REQUESTED_TOTAL.get(this);

		return null;
	}

	void upstreamRequest(Subscription s, long n) {
		long prev = Operators.addCap(REQUESTED_TOTAL, this, n);
		if (prev != Long.MAX_VALUE) {
			s.request(n);
		}
	}

	static final int MASK_TERMINATED       = 0b1000;
	static final int MASK_TERMINATING      = 0b0100;
	static final int MASK_ON_NEXT          = 0b0001;

	boolean checkTerminatedAfterOnNext() {
		int donePreviousState = markOnNextDone();
		if (isMarkedTerminating(donePreviousState)) {
			notifyDone();
			return true;
		}
		return false;
	}

	static boolean isMarkedTerminated(int state) {
		return (state & MASK_TERMINATED) == MASK_TERMINATED;
	}

	static boolean isMarkedOnNext(int state) {
		return (state & MASK_ON_NEXT) == MASK_ON_NEXT;
	}

	static boolean isMarkedTerminating(int state) {
		return (state & MASK_TERMINATING) == MASK_TERMINATING
				&& (state & MASK_TERMINATED) != MASK_TERMINATED;
	}

	/**
	 * Attempt to mark the TestSubscriber as terminated. Does nothing if already terminated.
	 * Mark as {@link #isMarkedTerminating(int)} if a concurrent onNext is detected.
	 *
	 * @return the previous state
	 */
	int markTerminated() {
		for(;;) {
			int state = this.state;
			if (isMarkedTerminated(state) || isMarkedTerminating(state)) {
				return state;
			}

			int newState;
			if (isMarkedOnNext(state)) {
				newState = state | MASK_TERMINATING;
			}
			else {
				newState = MASK_TERMINATED;
			}

			if (STATE.compareAndSet(this, state, newState)) {
				return state;
			}
		}
	}

	/**
	 * Mark that onNext processing has started (work in progress) and return the previous state.
	 * @return the previous state
	 */
	int markOnNextStart() {
		for(;;) {
			int state = this.state;
			if (state != 0) {
				return state;
			}

			if (STATE.compareAndSet(this, state, MASK_ON_NEXT)) {
				return state;
			}
		}
	}

	/**
	 * Mark that onNext processing has terminated and return the previous state.
	 * @return the previous state
	 */
	int markOnNextDone() {
		for(;;) {
			int state = this.state;
			int nextState = state & ~MASK_ON_NEXT;
			if (STATE.compareAndSet(this, state, nextState)) {
				return state;
			}
		}
	}

	// == public subscription-like methods

	@Override
	public void cancel() {
		if (cancelled.compareAndSet(false, true)) {
			if (this.s != null) {
				this.s.cancel();
			}

			if (requestedFusionMode == Fuseable.ASYNC) {
				int st = this.state;
				Fuseable.QueueSubscription<T> q = this.qs;
				if (!isMarkedOnNext(st) && q != null) {
					q.clear();
				}
			}

			notifyDone();
		}
	}

	@Override
	public void request(long n) {
		if (this.s == null) {
			for (;;) {
				long prevReq = REQUESTED_PRE_SUBSCRIPTION.get(this);
				if (prevReq == -1L) {
					request(n); //will propagate upstream
					return;
				}
				long newReq = Operators.addCap(prevReq, n);
				if (REQUESTED_PRE_SUBSCRIPTION.compareAndSet(this, prevReq, newReq)) {
					return;
				}
			}
		}

		if (Operators.validate(n)) {
			if (this.fusionMode == Fuseable.SYNC) {
				internalCancel();
				throw new IllegalStateException("Request is short circuited in SYNC fusion mode, and should not be explicitly used");
			}
			upstreamRequest(this.s, n);
		}
	}

	void checkSubscriptionFailure() {
		AssertionError subscriptionFailure = this.subscriptionFailure.get();
		if (subscriptionFailure != null) {
			throw subscriptionFailure;
		}
	}

	// == public accessors

	@Override
	public boolean isTerminatedOrCancelled() {
		checkSubscriptionFailure();
		return doneLatch.getCount() == 0;
	}

	@Override
	public boolean isTerminated() {
		checkSubscriptionFailure();
		return terminalSignal != null;
	}

	@Override
	public boolean isTerminatedComplete() {
		checkSubscriptionFailure();
		Signal<T> ts = this.terminalSignal;
		return ts != null && ts.isOnComplete();
	}

	@Override
	public boolean isTerminatedError() {
		checkSubscriptionFailure();
		Signal<T> ts = this.terminalSignal;
		return ts != null && ts.isOnError();
	}

	@Override
	public boolean isCancelled() {
		checkSubscriptionFailure();
		return cancelled.get();
	}

	@Override
	public @Nullable Signal<T> getTerminalSignal() {
		checkSubscriptionFailure();
		return this.terminalSignal;
	}

	@Override
	public Signal<T> expectTerminalSignal() {
		checkSubscriptionFailure();
		Signal<T> sig = this.terminalSignal;
		if (sig == null || (!sig.isOnError() && !sig.isOnComplete())) {
			cancel();
			throw new AssertionError("Expected subscriber to be terminated, but it has not been terminated yet: cancelling subscription.");
		}
		return sig;
	}

	@Override
	public Throwable expectTerminalError() {
		checkSubscriptionFailure();
		Signal<T> sig = this.terminalSignal;
		if (sig == null) {
			cancel();
			throw new AssertionError("Expected subscriber to have errored, but it has not been terminated yet.");
		}
		if (sig.isOnComplete()) {
			throw new AssertionError("Expected subscriber to have errored, but it has completed instead.");
		}
		Throwable terminal = sig.getThrowable();
		if (terminal == null) {
			cancel();
			throw new AssertionError("Expected subscriber to have errored, got unexpected terminal signal <" + sig + ">.");
		}
		return terminal;
	}

	@Override
	public List<T> getReceivedOnNext() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.receivedOnNext);
	}

	@Override
	public List<T> getReceivedOnNextAfterCancellation() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.receivedPostCancellation);
	}

	@Override
	public List<Signal<T>> getProtocolErrors() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.protocolErrors);
	}

	@Override
	public int getFusionMode() {
		checkSubscriptionFailure();
		return this.fusionMode;
	}


	// == blocking and awaiting termination

	@Override
	public void block() {
		try {
			this.doneLatch.await();
			checkSubscriptionFailure();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Block() interrupted", e);
		}
	}

	@Override
	public void block(Duration timeout) {
		long timeoutMs = timeout.toMillis();
		try {
			boolean done = this.doneLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
			checkSubscriptionFailure();
			if (!done) {
				throw new AssertionError("TestSubscriber timed out, not terminated after " + timeout + " (" + timeoutMs + "ms)");
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Block(" + timeout +") interrupted", e);
		}
	}
	//TODO should we add a method to await the latch without throwing ?
}

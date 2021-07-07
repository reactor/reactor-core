/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@link CoreSubscriber} that can be attached to any {@link org.reactivestreams.Publisher} to later assert which
 * events occurred at runtime. This can be used as an alternative to {@link reactor.test.StepVerifier}
 * for more complex scenarios, e.g. more than one possible outcome, racing...
 * <p>
 * The subscriber can be fine tuned with a {@link #builder()}, which also allows to produce a {@link reactor.core.Fuseable.ConditionalSubscriber}
 * variant if needed.
 * <p>
 * {@link org.reactivestreams.Subscriber}-inherited methods never throw, but a few failure conditions might be met, which
 * fall into two categories.
 * <p>
 * The first category are "protocol errors": when the occurrence of an incoming signal doesn't follow the Reactive Streams
 * specification. The case must be covered explicitly in the specification, and leads to the signal being added to the
 * {@link #getProtocolErrors()} list. All protocol errors imply that the {@link org.reactivestreams.Publisher} has terminated
 * already. The list of detected protocol errors is:
 * <ul>
 *     <li>the {@link TestSubscriber} has already terminated (onComplete or onError), but an {@link #onNext(Object)} is received: onNext signal added to protocol errors</li>
 *     <li>the {@link TestSubscriber} has already terminated, but an {@link #onComplete()} is received: onComplete signal added to protocol errors</li>
 *     <li>the {@link TestSubscriber} has already terminated, but an {@link #onError(Throwable)} is received: onError signal added to protocol errors</li>
 * </ul>
 * <p>
 * The second category are "subscription failures", which are the only ones for which {@link TestSubscriber} internally performs an assertion.
 * These failure conditions always lead to a cancellation of the subscription and are represented as an {@link AssertionError}.
 * The assertion error is thrown by all the {@link #getReceivedOnNext() getXxx} and {@link #isTerminated() isXxx} accessors, the {@link #block()} methods
 * and the {@link #expectTerminalError()}/{@link #expectTerminalSignal()} methods. The possible subscription failures are:
 * <ul>
 *     <li>the {@link TestSubscriber} has already received a {@link Subscription} (ie. it is being reused). Both subscriptions are cancelled.</li>
 *     <li>the incoming {@link Subscription} is not capable of fusion, but fusion was required by the user</li>
 *     <li>the incoming {@link Subscription} is capable of fusion, but this was forbidden by the user</li>
 *     <li>the incoming {@link Subscription} is capable of fusion, but the negotiated fusion mode is not the one required by the user</li>
 *     <li>onNext(null) is received, which should denote ASYNC fusion, but ASYNC fusion hasn't been established</li>
 * </ul>
 *
 * @author Simon Baslé
 */
public class TestSubscriber<T> implements CoreSubscriber<T>, Scannable {

	/**
	 * Create a simple plain {@link TestSubscriber} which will make an unbounded demand {@link #onSubscribe(Subscription) on subscription},
	 * has an empty {@link Context} and makes no attempt at fusion negotiation.
	 *
	 * @param <T> the type of data received by this subscriber
	 * @return a new {@link TestSubscriber}
	 */
	static <T> TestSubscriber<T> create() {
		return new TestSubscriber<>(new TestSubscriberBuilder());
	}

	/**
	 * An enum representing the 3 broad expectations around fusion.
	 */
	public enum FusionRequirement {

		/**
		 * The parent {@link org.reactivestreams.Publisher} is expected to be fuseable, and this is
		 * verified by checking the {@link Subscription} it provides is a {@link reactor.core.Fuseable.QueueSubscription}.
		 */
		FUSEABLE,
		/**
		 * The parent {@link org.reactivestreams.Publisher} is expected to NOT be fuseable, and this is
		 * verified by checking the {@link Subscription} it provides is NOT a {@link reactor.core.Fuseable.QueueSubscription}.
		 */
		NOT_FUSEABLE,
		/**
		 * There is no particular interest in the fuseability of the parent {@link org.reactivestreams.Publisher},
		 * so even if it provides a {@link reactor.core.Fuseable.QueueSubscription} it will be used as a
		 * vanilla {@link Subscription}.
		 */
		NONE;

	}

	/**
	 * Create a {@link TestSubscriber} with tuning. See {@link TestSubscriberBuilder}.
	 *
	 * @return a {@link TestSubscriberBuilder} to fine tune the {@link TestSubscriber} to produce
	 */
	public static TestSubscriberBuilder builder() {
		return new TestSubscriberBuilder();
	}

	final long                             initialRequest;
	final Context                          context;
	final TestSubscriber.FusionRequirement fusionRequirement;
	final int                              requestedFusionMode;
	final int                              expectedFusionMode;

	Subscription s;
	@Nullable
	Fuseable.QueueSubscription<T> qs;
	int fusionMode = -1;

	// state tracking
	final AtomicBoolean   cancelled;
	final List<T>         receivedOnNext;
	final List<T>         receivedPostCancellation;
	final List<Signal<T>> protocolErrors;

	final CountDownLatch                  doneLatch;
	final AtomicReference<AssertionError> subscriptionFailure;

	@Nullable
	volatile Signal<T> terminalSignal;

	volatile     int                                       state;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<TestSubscriber> STATE =
			AtomicIntegerFieldUpdater.newUpdater(TestSubscriber.class, "state");

	TestSubscriber(TestSubscriberBuilder options) {
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
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	void internalCancel() {
		Subscription s = this.s;
		if (cancelled.compareAndSet(false, true) && s != null) {
			s.cancel();
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
			return;
		}
		if (!Operators.validate(this.s, s)) {
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
			else if (this.initialRequest > 0L) {
				s.request(this.initialRequest);
			}
		}
		else if (fusionRequirement == FusionRequirement.FUSEABLE) {
			subscriptionFail("TestSubscriber configured to require QueueSubscription, got " + s);
		}
		else if (this.initialRequest > 0L) {
			s.request(this.initialRequest);
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
				this.protocolErrors.add(Signal.error(
						new AssertionError("onNext(null) received despite SYNC fusion (with concurrent onNext)")
				));
			}
			return;
		}

		if (t == null) {
			if (this.fusionMode == Fuseable.ASYNC) {
				assert this.qs != null;
				for (; ; ) {
					if (cancelled.get()) {
						checkTerminatedAfterOnNext();
						return;
					}
					t = qs.poll();
					if (t == null) {
						//no more available data, until next request (or termination)
						checkTerminatedAfterOnNext();
						return;
					}
					this.receivedOnNext.add(t);
				}
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
		notifyDone();
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
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

		return null;
	}

	static final int MASK_TERMINATED = 0b1000;
	static final int MASK_TERMINATING = 0b0100;
	static final int MASK_ON_NEXT = 0b0001;

	void checkTerminatedAfterOnNext() {
		int donePreviousState = markOnNextDone();
		if (isMarkedTerminating(donePreviousState)) {
			notifyDone();
		}
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

	/**
	 * Cancel the underlying subscription to the {@link org.reactivestreams.Publisher} and
	 * unblock any pending {@link #block()} calls.
	 */
	public void cancel() {
		if (cancelled.compareAndSet(false, true)) {
			if (this.s != null) {
				this.s.cancel();
			}
			notifyDone();
		}
	}

	/**
	 * Request {@code n} elements from the {@link org.reactivestreams.Publisher}'s {@link Subscription}.
	 * This method MUST only be called once the {@link TestSubscriber} has subscribed to the {@link org.reactivestreams.Publisher},
	 * otherwise an {@link IllegalStateException} is thrown.
	 * <p>
	 * Note that if {@link Fuseable#SYNC} fusion mode is established, this method shouldn't be used either, and this will
	 * also throw an {@link IllegalStateException}.
	 *
	 * @param n the additional amount to request
	 */
	public void request(long n) {
		if (this.s == null) {
			throw new IllegalStateException("Request can only happen once a Subscription has been established." +
					"Have you subscribed the TestSubscriber?");
		}
		if (Operators.validate(n)) {
			if (this.fusionMode == Fuseable.SYNC) {
				internalCancel();
				throw new IllegalStateException("Request is short circuited in SYNC fusion mode, and should not be explicitly used");
			}
			this.s.request(n);
		}
	}

	void checkSubscriptionFailure() {
		AssertionError subscriptionFailure = this.subscriptionFailure.get();
		if (subscriptionFailure != null) {
			throw subscriptionFailure;
		}
	}

	// == public accessors

	/**
	 * Check if this {@link TestSubscriber} has either:
	 * <ul>
	 *     <li>been cancelled: {@link #isCancelled()} would return true</li>
	 *     <li>been terminated, having been signalled with onComplete or onError: {@link #isTerminated()} would return true and {@link #getTerminalSignal()}
	 *     would return a non-null {@link Signal}</li>
	 * </ul>
	 * The third possible failure condition, subscription failure, results in an {@link AssertionError} being thrown by this method
	 * (like all other accessors, see also {@link TestSubscriber} javadoc).
	 * <p>
	 * Once this method starts returning true, any pending {@link #block()} calls should finish, and subsequent
	 * block calls will return immediately.
	 *
	 * @return true if the {@link TestSubscriber} has reached an end state
	 * @throws AssertionError in case of failure at subscription time
	 */
	public boolean isTerminatedOrCancelled() {
		checkSubscriptionFailure();
		return doneLatch.getCount() == 0;
	}

	/**
	 * Check if this {@link TestSubscriber} has received a terminal signal, ie. onComplete or onError.
	 * When returning {@code true}, implies:
	 * <ul>
	 *     <li>{@link #isTerminatedOrCancelled()} is also true</li>
	 *     <li>{@link #getTerminalSignal()} returns a non-null {@link Signal}</li>
	 *     <li>{@link #expectTerminalSignal()}} returns the {@link Signal}</li>
	 *     <li>{@link #expectTerminalError()}} returns the {@link Signal} in case of onError but throws in case of onComplete</li>
	 * </ul>
	 *
	 * @return true if the {@link TestSubscriber} has been terminated via onComplete or onError
	 * @throws AssertionError in case of failure at subscription time
	 */
	public boolean isTerminated() {
		checkSubscriptionFailure();
		return terminalSignal != null;
	}

	/**
	 * Check if this {@link TestSubscriber} has received a terminal signal that is specifically onComplete.
	 * When returning {@code true}, implies:
	 * <ul>
	 *     <li>{@link #isTerminatedOrCancelled()} is also true</li>
	 *     <li>{@link #isTerminated()} is also true</li>
	 *     <li>{@link #getTerminalSignal()} returns a non-null onComplete {@link Signal}</li>
	 *     <li>{@link #expectTerminalSignal()}} returns the same onComplete {@link Signal}</li>
	 *     <li>{@link #expectTerminalError()}} throws</li>
	 * </ul>
	 *
	 * @return true if the {@link TestSubscriber} has been terminated via onComplete
	 * @throws AssertionError in case of failure at subscription time
	 */
	public boolean isTerminatedComplete() {
		checkSubscriptionFailure();
		Signal<T> ts = this.terminalSignal;
		return ts != null && ts.isOnComplete();
	}

	/**
	 * Check if this {@link TestSubscriber} has received a terminal signal that is specifically onError.
	 * When returning {@code true}, implies:
	 * <ul>
	 *     <li>{@link #isTerminatedOrCancelled()} is also true</li>
	 *     <li>{@link #isTerminated()} is also true</li>
	 *     <li>{@link #getTerminalSignal()} returns a non-null onError {@link Signal}</li>
	 *     <li>{@link #expectTerminalSignal()}} returns the same onError {@link Signal}</li>
	 *     <li>{@link #expectTerminalError()}} returns the terminating {@link Throwable}</li>
	 * </ul>
	 *
	 * @return true if the {@link TestSubscriber} has been terminated via onComplete
	 * @throws AssertionError in case of failure at subscription time
	 */
	public boolean isTerminatedError() {
		checkSubscriptionFailure();
		Signal<T> ts = this.terminalSignal;
		return ts != null && ts.isOnError();
	}

	/**
	 * Check if this {@link TestSubscriber} has been {@link #cancel() cancelled}, which implies {@link #isTerminatedOrCancelled()} is also true.
	 *
	 * @return true if the {@link TestSubscriber} has been cancelled
	 * @throws AssertionError in case of failure at subscription time
	 */
	public boolean isCancelled() {
		checkSubscriptionFailure();
		return cancelled.get();
	}

	/**
	 * Return the terminal {@link Signal} if this {@link TestSubscriber} {@link #isTerminated()}, or {@code null} otherwise.
	 * See also {@link #expectTerminalSignal()} as a stricter way of asserting the terminal state.
	 *
	 * @return the terminal {@link Signal} or null if not terminated
	 * @see #isTerminated()
	 * @see #expectTerminalSignal()
	 * @throws AssertionError in case of failure at subscription time
	 */
	@Nullable
	public Signal<T> getTerminalSignal() {
		checkSubscriptionFailure();
		return this.terminalSignal;
	}

	/**
	 * Expect the {@link TestSubscriber} to be {@link #isTerminated() terminated}, and return the terminal {@link Signal}
	 * if so. Otherwise, <strong>cancel the subscription</strong> and throw an {@link AssertionError}.
	 * <p>
	 * Note that is there was already a subscription failure, the corresponding {@link AssertionError} is raised by this
	 * method instead.
	 *
	 * @return the terminal {@link Signal} (cannot be null)
	 * @see #isTerminated()
	 * @see #getTerminalSignal()
	 * @throws AssertionError in case of failure at subscription time, or if the subscriber hasn't terminated yet
	 */
	public Signal<T> expectTerminalSignal() {
		checkSubscriptionFailure();
		Signal<T> sig = this.terminalSignal;
		if (sig == null || (!sig.isOnError() && !sig.isOnComplete())) {
			cancel();
			throw new AssertionError("Expected subscriber to be terminated, but it has not been terminated yet: cancelling subscription.");
		}
		return sig;
	}

	/**
	 * Expect the {@link TestSubscriber} to be {@link #isTerminated() terminated} with an {@link #onError(Throwable)}
	 * and return the terminating {@link Throwable} if so.
	 * Otherwise, <strong>cancel the subscription</strong> and throw an {@link AssertionError}.
	 *
	 * @return the terminal {@link Throwable} (cannot be null)
	 * @see #isTerminated()
	 * @see #isTerminatedError()
	 * @see #getTerminalSignal()
	 * @throws AssertionError in case of failure at subscription time, or if the subscriber hasn't errored.
	 */
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

	/**
	 * Return the {@link List} of all elements that have correctly been emitted to the {@link TestSubscriber} (onNext signals)
	 * so far. This returns a new list that is not backed by the {@link TestSubscriber}.
	 * <p>
	 * Note that this includes elements that would arrive after {@link #cancel()}, as this is allowed by the Reactive Streams
	 * specification (cancellation is not necessarily synchronous and some elements may already be in flight when the source
	 * takes notice of the cancellation).
	 * These elements are also mirrored in the {@link #getReceivedOnNextAfterCancellation()} getter.
	 *
	 * @return the {@link List} of all elements received by the {@link TestSubscriber} as part of normal operation
	 * @see #getReceivedOnNextAfterCancellation()
	 * @see #getProtocolErrors()
	 * @throws AssertionError in case of failure at subscription time
	 */
	public List<T> getReceivedOnNext() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.receivedOnNext);
	}

	/**
	 * Return the {@link List} of elements that have been emitted to the {@link TestSubscriber} (onNext signals) so far,
	 * after a {@link #cancel()} was triggered. This returns a new list that is not backed by the {@link TestSubscriber}.
	 * <p>
	 * Note that this is allowed by the Reactive Streams specification (cancellation is not necessarily synchronous and
	 * some elements may already be in flight when the source takes notice of the cancellation).
	 * This is a sub-list of the one returned by {@link #getReceivedOnNext()} (in the conceptual sense, as the two lists
	 * are independent copies).
	 *
	 * @return the {@link List} of elements of {@link #getReceivedOnNext()} that were received by the {@link TestSubscriber}
	 * after {@link #cancel()} was triggered
	 * @see #getReceivedOnNext()
	 * @see #getProtocolErrors()
	 * @throws AssertionError in case of failure at subscription time
	 */
	public List<T> getReceivedOnNextAfterCancellation() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.receivedPostCancellation);
	}

	/**
	 * Return a {@link List} of {@link Signal} which represent detected protocol error from the source {@link org.reactivestreams.Publisher},
	 * that is to say signals that were emitted to this {@link TestSubscriber} in violation of the Reactive Streams
	 * specification. An example would be an {@link #onNext(Object)} signal emitted after an {@link #onComplete()} signal.
	 * <p>
	 * Note that the {@link Signal} in the collection don't bear any {@link reactor.util.context.ContextView},
	 * since they would all be the configured {@link #currentContext()}.
	 *
	 * @return a {@link List} of {@link Signal} representing the detected protocol errors from the source {@link org.reactivestreams.Publisher}
	 * @throws AssertionError in case of failure at subscription time
	 */
	public List<Signal<T>> getProtocolErrors() {
		checkSubscriptionFailure();
		return new ArrayList<>(this.protocolErrors);
	}

	/**
	 * Return an {@code int} code that represents the negotiated fusion mode for this {@link TestSubscriber}.
	 * Fusion codes can be converted to a human-readable value for display via {@link Fuseable#fusionModeName(int)}.
	 * If no particular fusion has been requested, returns {@link Fuseable#NONE}.
	 * Note that as long as this {@link TestSubscriber} hasn't been subscribed to a {@link org.reactivestreams.Publisher},
	 * this method will return {@code -1}. It will also throw an {@link AssertionError} if the configured fusion mode
	 * couldn't be negotiated at subscription.
	 *
	 * @return -1 if not subscribed, 0 ({@link Fuseable#NONE}) if no fusion negotiated, a relevant fusion code otherwise
	 * @throws AssertionError in case of failure at subscription time
	 */
	public int getFusionMode() {
		checkSubscriptionFailure();
		return this.fusionMode;
	}


	// == blocking and awaiting termination

	/**
	 * Block until an assertable end state has been reached. This can be either a cancellation ({@link #isCancelled()}),
	 * a "normal" termination ({@link #isTerminated()}) or subscription failure. In the later case only, this method
	 * throws the corresponding {@link AssertionError}.
	 * <p>
	 * An AssertionError is also thrown if the thread is interrupted.
	 *
	 * @throws AssertionError in case of failure at subscription time (or thread interruption)
	 */
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

	/**
	 * Block until an assertable end state has been reached, or a timeout {@link Duration} has elapsed.
	 * End state can be either a cancellation ({@link #isCancelled()}), a "normal" termination ({@link #isTerminated()})
	 * or a subscription failure. In the later case only, this method throws the corresponding {@link AssertionError}.
	 * In case of timeout, an {@link AssertionError} with a message reflecting the configured duration is thrown.
	 * <p>
	 * An AssertionError is also thrown if the thread is interrupted.
	 *
	 * @throws AssertionError in case of failure at subscription time (or thread interruption)
	 */
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

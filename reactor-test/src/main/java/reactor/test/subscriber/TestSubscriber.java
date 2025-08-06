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
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Signal;
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
public interface TestSubscriber<T> extends CoreSubscriber<T>, Scannable {

	/**
	 * Create a simple plain {@link TestSubscriber} which will make an unbounded demand {@link #onSubscribe(Subscription) on subscription},
	 * has an empty {@link Context} and makes no attempt at fusion negotiation.
	 *
	 * @param <T> the type of data received by this subscriber
	 * @return a new plain {@link TestSubscriber}
	 */
	static <T> TestSubscriber<T> create() {
		return new DefaultTestSubscriber<>(new TestSubscriberBuilder());
	}

	/**
	 * Create a {@link TestSubscriber} with tuning. See {@link TestSubscriberBuilder}.
	 *
	 * @return a {@link TestSubscriberBuilder} to fine tune the {@link TestSubscriber} to produce
	 */
	static TestSubscriberBuilder builder() {
		return new TestSubscriberBuilder();
	}

	/**
	 * Cancel the underlying subscription to the {@link org.reactivestreams.Publisher} and
	 * unblock any pending {@link #block()} calls.
	 */
	void cancel();

	/**
	 * Request {@code n} elements from the {@link org.reactivestreams.Publisher}'s {@link Subscription}.
	 * If this method is called before the {@link TestSubscriber} has subscribed to the {@link org.reactivestreams.Publisher},
	 * pre-request is accumulated (including {@link TestSubscriberBuilder#initialRequest(long) configured initial request}
	 * and replayed in a single batch upon subscription.
	 * <p>
	 * Note that if/once {@link Fuseable#SYNC} fusion mode is established, this method MUST NOT be used, and this will
	 * throw an {@link IllegalStateException}.
	 *
	 * @param n the additional amount to request
	 */
	void request(long n);

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	boolean isTerminatedOrCancelled();

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	boolean isTerminated();

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	boolean isTerminatedComplete();

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	boolean isTerminatedError();

	/**
	 * Check if this {@link TestSubscriber} has been {@link #cancel() cancelled}, which implies {@link #isTerminatedOrCancelled()} is also true.
	 *
	 * @return true if the {@link TestSubscriber} has been cancelled
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	boolean isCancelled();

	/**
	 * Return the terminal {@link Signal} if this {@link TestSubscriber} {@link #isTerminated()}, or {@code null} otherwise.
	 * See also {@link #expectTerminalSignal()} as a stricter way of asserting the terminal state.
	 *
	 * @return the terminal {@link Signal} or null if not terminated
	 *
	 * @throws AssertionError in case of failure at subscription time
	 * @see #isTerminated()
	 * @see #expectTerminalSignal()
	 */
	@Nullable
	Signal<T> getTerminalSignal();

	/**
	 * Expect the {@link TestSubscriber} to be {@link #isTerminated() terminated}, and return the terminal {@link Signal}
	 * if so. Otherwise, <strong>cancel the subscription</strong> and throw an {@link AssertionError}.
	 * <p>
	 * Note that is there was already a subscription failure, the corresponding {@link AssertionError} is raised by this
	 * method instead.
	 *
	 * @return the terminal {@link Signal} (cannot be null)
	 *
	 * @throws AssertionError in case of failure at subscription time, or if the subscriber hasn't terminated yet
	 * @see #isTerminated()
	 * @see #getTerminalSignal()
	 */
	Signal<T> expectTerminalSignal();

	/**
	 * Expect the {@link TestSubscriber} to be {@link #isTerminated() terminated} with an {@link #onError(Throwable)}
	 * and return the terminating {@link Throwable} if so.
	 * Otherwise, <strong>cancel the subscription</strong> and throw an {@link AssertionError}.
	 *
	 * @return the terminal {@link Throwable} (cannot be null)
	 *
	 * @throws AssertionError in case of failure at subscription time, or if the subscriber hasn't errored.
	 * @see #isTerminated()
	 * @see #isTerminatedError()
	 * @see #getTerminalSignal()
	 */
	Throwable expectTerminalError();

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 * @see #getReceivedOnNextAfterCancellation()
	 * @see #getProtocolErrors()
	 */
	List<T> getReceivedOnNext();

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
	 *
	 * @throws AssertionError in case of failure at subscription time
	 * @see #getReceivedOnNext()
	 * @see #getProtocolErrors()
	 */
	List<T> getReceivedOnNextAfterCancellation();

	/**
	 * Return a {@link List} of {@link Signal} which represent detected protocol error from the source {@link org.reactivestreams.Publisher},
	 * that is to say signals that were emitted to this {@link TestSubscriber} in violation of the Reactive Streams
	 * specification. An example would be an {@link #onNext(Object)} signal emitted after an {@link #onComplete()} signal.
	 * <p>
	 * Note that the {@link Signal} in the collection don't bear any {@link reactor.util.context.ContextView},
	 * since they would all be the configured {@link #currentContext()}.
	 *
	 * @return a {@link List} of {@link Signal} representing the detected protocol errors from the source {@link org.reactivestreams.Publisher}
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	List<Signal<T>> getProtocolErrors();

	/**
	 * Return an {@code int} code that represents the negotiated fusion mode for this {@link TestSubscriber}.
	 * Fusion codes can be converted to a human-readable value for display via {@link Fuseable#fusionModeName(int)}.
	 * If no particular fusion has been requested, returns {@link Fuseable#NONE}.
	 * Note that as long as this {@link TestSubscriber} hasn't been subscribed to a {@link org.reactivestreams.Publisher},
	 * this method will return {@code -1}. It will also throw an {@link AssertionError} if the configured fusion mode
	 * couldn't be negotiated at subscription.
	 *
	 * @return -1 if not subscribed, 0 ({@link Fuseable#NONE}) if no fusion negotiated, a relevant fusion code otherwise
	 *
	 * @throws AssertionError in case of failure at subscription time
	 */
	int getFusionMode();

	/**
	 * Block until an assertable end state has been reached. This can be either a cancellation ({@link #isCancelled()}),
	 * a "normal" termination ({@link #isTerminated()}) or subscription failure. In the later case only, this method
	 * throws the corresponding {@link AssertionError}.
	 * <p>
	 * An AssertionError is also thrown if the thread is interrupted.
	 *
	 * @throws AssertionError in case of failure at subscription time (or thread interruption)
	 */
	void block();

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
	void block(Duration timeout);

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
}

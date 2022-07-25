/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.observability;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

/**
 * A listener which combines various handlers to be triggered per the corresponding {@link Flux} or {@link Mono} signals.
 * This is similar to the "side effect" operators in {@link Flux} and {@link Mono}, but in a single listener class.
 * {@link SignalListener} are created by a {@link SignalListenerFactory}, which is tied to a particular {@link Publisher}.
 * Each time a new {@link Subscriber} subscribes to that {@link Publisher}, the factory creates an associated {@link SignalListener}.
 * <p>
 * Both publisher-to-subscriber events and subscription events are handled. Methods are closer to the side-effect doOnXxx operators
 * than to {@link Subscriber} and {@link Subscription} methods, in order to avoid misconstruing this for an actual Reactive Streams
 * implementation. The actual downstream {@link Subscriber} and upstream {@link Subscription} are intentionally not exposed
 * to avoid any influence on the observed sequence.
 *
 * @author Simon Baslé
 */
public interface SignalListener<T> {

	/**
	 * Handle the very beginning of the {@link Subscriber}-{@link Publisher} interaction.
	 * This handler is invoked right before subscribing to the parent {@link Publisher}, as a downstream
	 * {@link Subscriber} has called {@link Publisher#subscribe(Subscriber)}.
	 * <p>
	 * Once the {@link Publisher} has acknowledged with a {@link Subscription}, the {@link #doOnSubscription()}
	 * handler will be invoked before that {@link Subscription} is passed down.
	 *
	 * @see #doOnSubscription()
	 */
	void doFirst() throws Throwable;

	/**
	 * Handle terminal signals after the signals have been propagated, as the final step.
	 * Only {@link SignalType#ON_COMPLETE}, {@link SignalType#ON_ERROR} or {@link SignalType#CANCEL} can be passed.
	 * This handler is invoked AFTER the terminal signal has been propagated, and if relevant AFTER the {@link #doAfterComplete()}
	 * or {@link #doAfterError(Throwable)} events. If any doOnXxx handler throws, this handler is NOT invoked (see {@link #handleListenerError(Throwable)}
	 * instead).
	 *
	 * @see #handleListenerError(Throwable)
	 */
	void doFinally(SignalType terminationType) throws Throwable;

	/**
	 * Handle the fact that the upstream {@link Publisher} acknowledged {@link Subscription}.
	 * The {@link Subscription} is intentionally not exposed in order to avoid manipulation by the observer.
	 * <p>
	 * While {@link #doFirst} is invoked right as the downstream {@link Subscriber} is registered,
	 * this method is invoked as the upstream answers back with a {@link Subscription} (and before that
	 * same {@link Subscription} is passed downstream).
	 *
	 * @see #doFirst()
	 */
	void doOnSubscription() throws Throwable;

	/**
	 * Handle the negotiation of fusion between two {@link reactor.core.Fuseable} operators. As the downstream operator
	 * requests fusion, the upstream answers back with the compatible level of fusion it can handle. This {@code negotiatedFusion}
	 * code is passed to this handler right before it is propagated downstream.
	 *
	 * @param negotiatedFusion the final fusion mode negotiated by the upstream operator in response to a fusion request
	 * from downstream
	 */
	void doOnFusion(int negotiatedFusion) throws Throwable;

	/**
	 * Handle a new request made by the downstream, exposing the demand.
	 * <p>
	 * This is invoked before the request is propagated upstream.
	 *
	 * @param requested the downstream demand
	 */
	void doOnRequest(long requested) throws Throwable;

	/**
	 * Handle the downstream cancelling its currently observed {@link Subscription}.
	 * <p>
	 * This handler is invoked before propagating the cancellation upstream, while {@link #doFinally(SignalType)}
	 * is invoked right after the cancellation has been propagated upstream.
	 *
	 * @see #doFinally(SignalType)
	 */
	void doOnCancel() throws Throwable;

	/**
	 * Handle a new value emission from the source.
	 * <p>
	 * This handler is invoked before propagating the value downstream.
	 *
	 * @param value the emitted value
	 */
	void doOnNext(T value) throws Throwable;

	/**
	 * Handle graceful onComplete sequence termination.
	 * <p>
	 * This handler is invoked before propagating the completion downstream, while both
	 * {@link #doAfterComplete()} and {@link #doFinally(SignalType)} are invoked after.
	 *
	 * @see #doAfterComplete()
	 * @see #doFinally(SignalType)
	 */
	void doOnComplete() throws Throwable;

	/**
	 * Handle onError sequence termination.
	 * <p>
	 * This handler is invoked before propagating the error downstream, while both
	 * {@link #doAfterError(Throwable)} and {@link #doFinally(SignalType)} are invoked after.
	 *
	 * @param error the exception that terminated the sequence
	 * @see #doAfterError(Throwable)
	 * @see #doFinally(SignalType)
	 */
	void doOnError(Throwable error) throws Throwable;

	/**
	 * Handle graceful onComplete sequence termination, after onComplete has been propagated downstream.
	 * <p>
	 * This handler is invoked after propagating the completion downstream, similar to {@link #doFinally(SignalType)}
	 * and unlike {@link #doOnComplete()}.
	 */
	void doAfterComplete() throws Throwable;

	/**
	 * Handle onError sequence termination after onError has been propagated downstream.
	 * <p>
	 * This handler is invoked after propagating the error downstream, similar to {@link #doFinally(SignalType)}
	 * and unlike {@link #doOnError(Throwable)}.
	 *
	 * @param error the exception that terminated the sequence
	 */
	void doAfterError(Throwable error) throws Throwable;

	/**
	 * Handle malformed {@link Subscriber#onNext(Object)}, which are onNext happening after the sequence has already terminated
	 * via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * Note that after this handler is invoked, the value is automatically {@link Operators#onNextDropped(Object, Context) dropped}.
	 * <p>
	 * If this handler fails with an exception, that exception is {@link Operators#onErrorDropped(Throwable, Context) dropped} before the
	 * value is also dropped.
	 *
	 * @param value the value for which an emission was attempted (which will be automatically dropped afterwards)
	 */
	void doOnMalformedOnNext(T value) throws Throwable;

	/**
	 * Handle malformed {@link Subscriber#onError(Throwable)}, which means the sequence has already terminated
	 * via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * Note that after this handler is invoked, the exception is automatically {@link Operators#onErrorDropped(Throwable, Context) dropped}.
	 * <p>
	 * If this handler fails with an exception, that exception is {@link Operators#onErrorDropped(Throwable, Context) dropped} before the
	 * original onError exception is also dropped.
	 *
	 * @param error the extraneous exception (which will be automatically dropped afterwards)
	 */
	void doOnMalformedOnError(Throwable error) throws Throwable;

	/**
	 * Handle malformed {@link Subscriber#onComplete()}, which means the sequence has already terminated
	 * via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * <p>
	 * If this handler fails with an exception, that exception is {@link Operators#onErrorDropped(Throwable, Context) dropped}.
	 */
	void doOnMalformedOnComplete() throws Throwable;

	/**
	 * A special handler for exceptions thrown from all the other handlers.
	 * This method MUST return normally, i.e. it MUST NOT throw.
	 * When a {@link SignalListener} handler fails, callers are expected to first invoke this method then to propagate
	 * the {@code listenerError} downstream if that is possible, terminating the original sequence with the listenerError.
	 * <p>
	 * Typically, this special handler is intended for a last chance at processing the error despite the fact that
	 * {@link #doFinally(SignalType)} is not triggered on handler errors. For example, recording the error in a
	 * metrics backend or cleaning up state that would otherwise be cleaned up by {@link #doFinally(SignalType)}.
	 *
	 * @param listenerError the exception thrown from a {@link SignalListener} handler method
	 */
	void handleListenerError(Throwable listenerError);

	/**
	 * In some cases, the tap operation should alter the {@link Context} exposed by the operator in order to store additional
	 * data. This method is invoked when the tap subscriber is created, which is between the invocation of {@link #doFirst()}
	 * and the invocation of {@link #doOnSubscription()}. Generally, only addition of new keys should be performed on
	 * the downstream original {@link Context}. Extra care should be exercised if any pre-existing key is to be removed
	 * or replaced.
	 *
	 * @param originalContext the original downstream operator's {@link Context}
	 * @return the {@link Context} to use and expose upstream
	 */
	default Context addToContext(Context originalContext) {
		return originalContext;
	}
}

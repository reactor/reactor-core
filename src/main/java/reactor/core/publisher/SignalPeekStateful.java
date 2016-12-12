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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Receiver;

/**
 * Peek into the lifecycle and sequence signals, with a state supplier to be
 * invoked on each subscription, resulting in a mutable state holder that will
 * be passed to the callbacks.
 * <p>
 * The callbacks are all optional.
 *
 * @param <T> the value type of the sequence
 * @param <S> the type of the state object
 */
interface SignalPeekStateful<T, S> extends Receiver {

	/**
	 * A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
	 */
	BiConsumer<? super Subscription, S> onSubscribeCall();

	/**
	 * A consumer that will observe {@link Subscriber#onNext(Object)}
	 *
	 * @return A consumer that will observe {@link Subscriber#onNext(Object)}
	 */
	BiConsumer<? super T, S> onNextCall();

	/**
	 * A consumer that will observe {@link Subscriber#onError(Throwable)}}
	 *
	 * @return A consumer that will observe {@link Subscriber#onError(Throwable)}
	 */
	BiConsumer<? super Throwable, S> onErrorCall();

	/**
	 * A task that will run on {@link Subscriber#onComplete()}
	 *
	 * @return A task that will run on {@link Subscriber#onComplete()}
	 */
	Consumer<S> onCompleteCall();

	/**
	 * A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
	 *
	 * @return A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
	 */
	Consumer<S> onAfterTerminateCall();

	/**
	 * A consumer of long that will observe {@link Subscription#request(long)}}
	 *
	 * @return A consumer of long that will observe {@link Subscription#request(long)}}
	 */
	BiConsumer<Long, S> onRequestCall();

	/**
	 * A task that will run on {@link Subscription#cancel()}
	 *
	 * @return A task that will run on {@link Subscription#cancel()}
	 */
	Consumer<S> onCancelCall();

	/**
	 * A task that will run after (finally) {@link Subscriber#onNext(Object)}
	 * @return A task that will run after (finally) {@link Subscriber#onNext(Object)}
	 */
	default BiConsumer<? super T, S> onAfterNextCall(){
		return null;
	}

	@Override
	Publisher<? extends T> upstream();

	/**
	 * Common method for {@link SignalPeekStateful} to deal with a doAfterTerminate
	 * callback that fails during onComplete. It invokes the error callback but
	 * protects against the error callback also failing.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Throwable)} is called</li>
	 *     <li>An attempt to execute the error callback is made</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable)} is called</li>
	 * </ul>
	 * <p>
	 * Note that if the error callback fails too, its exception is made to
	 * suppress the afterTerminate callback exception, and then onErrorDropped.
	 *
	 * @param parent the {@link SignalPeekStateful} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 * @param state the state object of the {@link SignalPeekStateful} subscriber
	 */
	static <T, S> void afterCompleteWithFailure(SignalPeekStateful<T, S> parent,
			Throwable callbackFailure, S state) {
		Exceptions.throwIfFatal(callbackFailure);
		Throwable e = Operators.onOperatorError(callbackFailure);
		try {
			if(parent.onErrorCall() != null) {
				parent.onErrorCall().accept(e, state);
			}
			Operators.onErrorDropped(e);
		}
		catch (Throwable t) {
			t.addSuppressed(e);
			Operators.onErrorDropped(t);
		}
	}

	/**
	 * Common method for {@link SignalPeekStateful} to deal with a doAfterTerminate
	 * callback that fails during onError. It invokes the error callback but protects
	 * against the error callback also failing.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Subscription, Throwable, Object)} is
	 *     called, adding the original error as suppressed</li>
	 *     <li>An attempt to execute the error callback is made</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable)} is called</li>
	 * </ul>
	 * <p>
	 * Note that if the error callback fails too, its exception is made to
	 * suppress both the decorated afterTerminate callback exception and the original
	 * error, and then onErrorDropped.
	 *
	 * @param parent the {@link SignalPeekStateful} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 * @param originalError the onError throwable
	 * @param state the state object of the {@link SignalPeekStateful} subscriber
	 */
	static <T, S> void afterErrorWithFailure(SignalPeekStateful<T, S> parent,
			Throwable callbackFailure, Throwable originalError, S state) {
		Exceptions.throwIfFatal(callbackFailure);
		Throwable _e = Operators.onOperatorError(null, callbackFailure, originalError);
		try {
			if (parent.onErrorCall() != null) {
				parent.onErrorCall().accept(_e, state);
			}
			Operators.onErrorDropped(_e);
		}
		catch (Throwable t) {
			t.addSuppressed(_e);
			t.addSuppressed(originalError);
			Operators.onErrorDropped(t);
		}
	}
}

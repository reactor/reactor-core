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

}

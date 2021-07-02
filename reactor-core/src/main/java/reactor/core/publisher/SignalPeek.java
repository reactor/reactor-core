/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Peek into the lifecycle and sequence signals.
 * <p>
 * The callbacks are all optional.
 *
 * @param <T> the value type of the sequence
 */
interface SignalPeek<T> extends Scannable {

	/**
	 * A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
	 */
	@Nullable
	Consumer<? super Subscription> onSubscribeCall();

	/**
	 * A consumer that will observe {@link Subscriber#onNext(Object)}
	 *
	 * @return A consumer that will observe {@link Subscriber#onNext(Object)}
	 */
	@Nullable
	Consumer<? super T> onNextCall();

	/**
	 * A consumer that will observe {@link Subscriber#onError(Throwable)}}
	 *
	 * @return A consumer that will observe {@link Subscriber#onError(Throwable)}
	 */
	@Nullable
	Consumer<? super Throwable> onErrorCall();

	/**
	 * A task that will run on {@link Subscriber#onComplete()}
	 *
	 * @return A task that will run on {@link Subscriber#onComplete()}
	 */
	@Nullable
	Runnable onCompleteCall();

	/**
	 * A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
	 *
	 * @return A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
	 */
	@Nullable
	Runnable onAfterTerminateCall();

	/**
	 * A consumer of long that will observe {@link Subscription#request(long)}}
	 *
	 * @return A consumer of long that will observe {@link Subscription#request(long)}}
	 */
	@Nullable
	LongConsumer onRequestCall();

	/**
	 * A task that will run on {@link Subscription#cancel()}
	 *
	 * @return A task that will run on {@link Subscription#cancel()}
	 */
	@Nullable
	Runnable onCancelCall();

	/**
	 * A task that will run after (finally) {@link Subscriber#onNext(Object)}
	 * @return A task that will run after (finally) {@link Subscriber#onNext(Object)}
	 */
	@Nullable
	default Consumer<? super T> onAfterNextCall(){
		return null;
	}

	/**
	 * A task that will run on {@link Context} read from downstream to upstream
	 * @return A task that will run on {@link Context} propagation from upstream to downstream
	 */
	@Nullable
	default Consumer<? super Context> onCurrentContextCall(){
		return null;
	}
}

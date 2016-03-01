/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Subscription;

/**
 * Set of methods that return various publisher state peeking callbacks.
 *
 * @param <T> the value type of the sequence
 */
interface FluxPeekHelper<T> {

	Consumer<? super Subscription> onSubscribeCall();

	Consumer<? super T> onNextCall();

	Consumer<? super Throwable> onErrorCall();

	Runnable onCompleteCall();

	Runnable onAfterTerminateCall();

	LongConsumer onRequestCall();

	Runnable onCancelCall();
}
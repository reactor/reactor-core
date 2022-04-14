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

package reactor.core.publisher;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxListen.ListenSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.observability.SignalListener;
import reactor.util.observability.SignalListenerFactory;

/**
 * A generic per-Subscription side effect {@link Mono} that notifies a {@link SignalListener} of most events.
 *
 * @author Simon Baslé
 */
final class MonoListen<T, STATE> extends InternalMonoOperator<T, T> {

	final SignalListenerFactory<T, STATE> factory;
	final STATE                           publisherState;

	MonoListen(Mono<? extends T> source, SignalListenerFactory<T, STATE> factory) {
		super(source);
		this.factory = factory;
		this.publisherState = factory.initializePublisherState(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) throws Throwable {
		//if the SequenceObserver cannot be created, all we can do is error the subscriber.
		//after it is created, in case doFirst fails we can additionally try to invoke doFinally.
		//note that if the later handler also fails, then that exception is thrown.
		SignalListener<T> signalListener;
		try {
			//TODO replace currentContext() with contextView() when available
			signalListener = factory.createListener(source, actual.currentContext().readOnly(), publisherState);
		}
		catch (Throwable generatorError) {
			Operators.error(actual, generatorError);
			return null;
		}

		try {
			signalListener.doFirst();
		}
		catch (Throwable observerError) {
			Operators.error(actual, observerError);
			signalListener.doFinally(SignalType.ON_ERROR);
			return null;
		}

		if (actual instanceof Fuseable.ConditionalSubscriber) {
			//noinspection unchecked
			return new FluxListen.ListenConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>) actual, signalListener);
		}
		return new ListenSubscriber<>(actual, signalListener);
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}
}

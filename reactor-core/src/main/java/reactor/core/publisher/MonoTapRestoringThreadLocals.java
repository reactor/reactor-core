/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.context.ContextSnapshot;
import reactor.core.CoreSubscriber;
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A generic per-Subscription side effect {@link Mono} that notifies a {@link SignalListener} of most events.
 *
 * @author Simon Baslé
 */
final class MonoTapRestoringThreadLocals<T, STATE> extends MonoOperator<T, T> {

	final SignalListenerFactory<T, STATE> tapFactory;
	final STATE                           commonTapState;

	MonoTapRestoringThreadLocals(Mono<? extends T> source, SignalListenerFactory<T, STATE> tapFactory) {
		super(source);
		this.tapFactory = tapFactory;
		this.commonTapState = tapFactory.initializePublisherState(source);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		//if the SignalListener cannot be created, all we can do is error the subscriber.
		//after it is created, in case doFirst fails we can additionally try to invoke doFinally.
		//note that if the later handler also fails, then that exception is thrown.
		SignalListener<T> signalListener;
		try {
			//TODO replace currentContext() with contextView() when available
			signalListener = tapFactory.createListener(source, actual.currentContext().readOnly(), commonTapState);
		}
		catch (Throwable generatorError) {
			Operators.error(actual, generatorError);
			return;
		}

		try {
			signalListener.doFirst();
		}
		catch (Throwable listenerError) {
			signalListener.handleListenerError(listenerError);
			Operators.error(actual, listenerError);
			return;
		}

		// invoked AFTER doFirst
		Context alteredContext;
		try {
			alteredContext = signalListener.addToContext(actual.currentContext());
		}
		catch (Throwable e) {
			signalListener.handleListenerError(new IllegalStateException("Unable to augment tap Context at construction via addToContext", e));
			alteredContext = actual.currentContext();
		}

		try (ContextSnapshot.Scope ignored = ContextPropagation.setThreadLocals(alteredContext)) {
			source.subscribe(new FluxTapRestoringThreadLocals.TapSubscriber<>(actual, signalListener, alteredContext));
		}
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return -1;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}
}

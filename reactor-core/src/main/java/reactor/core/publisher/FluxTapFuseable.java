/*
 * Copyright (c) 2022-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.util.context.Context;

/**
 * A {@link reactor.core.Fuseable} generic per-Subscription side effect {@link Flux} that notifies a
 * {@link SignalListener} of most events.
 *
 * @author Simon Baslé
 */
final class FluxTapFuseable<T, STATE> extends InternalFluxOperator<T, T> implements Fuseable {

	final SignalListenerFactory<T, STATE> tapFactory;
	final STATE                           commonTapState;

	FluxTapFuseable(Flux<? extends T> source, SignalListenerFactory<T, STATE> tapFactory) {
		super(source);
		this.tapFactory = tapFactory;
		this.commonTapState = tapFactory.initializePublisherState(source);
	}

	@Override
	public @Nullable CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) throws Throwable {
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
			return null;
		}
		// Attempt to wrap the SignalListener with one that restores ThreadLocals from Context on each listener methods
		// (only if ContextPropagation.isContextPropagationAvailable() is true)
		signalListener = ContextPropagationSupport.isContextPropagationAvailable() ?
				ContextPropagation.contextRestoreForTap(signalListener, actual::currentContext) : signalListener;

		try {
			signalListener.doFirst();
		}
		catch (Throwable listenerError) {
			signalListener.handleListenerError(listenerError);
			Operators.error(actual, listenerError);
			return null;
		}

		// Invoked AFTER doFirst
		Context ctx;
		try {
			ctx = signalListener.addToContext(actual.currentContext());
		}
		catch (Throwable e) {
			IllegalStateException listenerError = new IllegalStateException(
					"Unable to augment tap Context at subscription via addToContext", e);
			signalListener.handleListenerError(listenerError);
			Operators.error(actual, listenerError);
			return null;
		}

		if (actual instanceof ConditionalSubscriber) {
			//noinspection unchecked
			return new TapConditionalFuseableSubscriber<>(
					(ConditionalSubscriber<? super T>) actual, signalListener, ctx);
		}
		return new TapFuseableSubscriber<>(actual, signalListener, ctx);
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}

	//TODO support onErrorContinue around listener errors
	static class TapFuseableSubscriber<T> extends FluxTap.TapSubscriber<T> implements QueueSubscription<T> {

		int mode;

		@SuppressWarnings("NotNullFieldNotInitialized") // qs initialized in onSubscribe
		QueueSubscription<T> qs;

		TapFuseableSubscriber(CoreSubscriber<? super T> actual,
				SignalListener<T> signalListener, Context ctx) {
			super(actual, signalListener, ctx);
		}

		/**
		 * Cancel the active subscription, pass the listener error to {@link SignalListener#handleListenerError(Throwable)}
		 * then return that same exception wrapped via {@link Exceptions#propagate(Throwable)} if needed.
		 * It should be immediately thrown to terminate the downstream directly from {@link #poll()} (without invoking
		 * any other handler).
		 *
		 * @param listenerError the exception thrown from a handler method
		 */
		protected RuntimeException handleObserverErrorInPoll(Throwable listenerError) {
			qs.cancel();
			listener.handleListenerError(listenerError);
			return Exceptions.propagate(listenerError);
		}

		/**
		 * Cancel the active subscription, pass the listener error to {@link SignalListener#handleListenerError(Throwable)},
		 * combine it with the original error and then return the combined exception. The returned exception should be
		 * immediately thrown to terminate the downstream directly from {@link #poll()} (without invoking any other handler).
		 *
		 * @param listenerError the exception thrown from a handler method
		 * @param pollError the exception that was about to be thrown from poll when handler was invoked
		 */
		protected RuntimeException handleObserverErrorMultipleInPoll(Throwable listenerError, RuntimeException pollError) {
			qs.cancel();
			listener.handleListenerError(listenerError);
			return Exceptions.multiple(listenerError, pollError);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				if (!(s instanceof QueueSubscription)) {
					handleListenerErrorPreSubscription(new IllegalStateException("Fuseable subscriber but no QueueSubscription"), s);
					return;
				}
				this.s = s;

				@SuppressWarnings("unchecked")
				QueueSubscription<T> qs = (QueueSubscription<T>) s;

				this.qs = qs;

				try {
					listener.doOnSubscription();
				}
				catch (Throwable listenerError) {
					handleListenerErrorPreSubscription(listenerError, s);
					return;
				}
				actual.onSubscribe(this); //should trigger requestFusion
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (qs == null) {
				this.mode = NONE;
				return NONE;
			}
			this.mode = qs.requestFusion(requestedMode);

			try {
				listener.doOnFusion(this.mode);
				return mode;
			}
			catch (Throwable listenerError) {
				if (mode == ASYNC || mode == NONE) {
					handleListenerErrorAndTerminate(listenerError);
					return NONE;
				}
				//for SYNC, no interruption
				listener.handleListenerError(listenerError);
				return mode;
			}
		}

		@SuppressWarnings("DataFlowIssue") // fusion passes nulls via onNext
		@Override
		public void onNext(T t) {
			if (this.mode == ASYNC) {
				actual.onNext(null);
				return; //will observe onNext events through the lens of poll()
			}
			super.onNext(t);
		}

		@Override
		public @Nullable T poll() {
			if (qs == null) {
				return null;
			}
			T v;
			//try to poll. failure means doOnError. doOnError failure is combined with original
			try {
				v = qs.poll();
			}
			catch (RuntimeException pollError) {
				try {
					listener.doOnError(pollError);
				}
				catch (Throwable listenerError) {
					throw handleObserverErrorMultipleInPoll(listenerError, pollError);
				}

				//the subscription can be considered cancelled at this point
				//exceptionally we invoked doFinally _before_ the propagation (since it is throwing)
				try {
					listener.doFinally(SignalType.ON_ERROR);
				}
				catch (Throwable listenerError) {
					throw handleObserverErrorMultipleInPoll(listenerError, pollError);
				}
				throw pollError;
			}

			//SYNC fusion uses null as onComplete and throws as onError
			//ASYNC fusion uses classic methods
			if (v == null && (this.done || mode == SYNC)) {
				try {
					listener.doOnComplete();
				}
				catch (Throwable listenerError) {
					throw handleObserverErrorInPoll(listenerError);
				}

				//exceptionally doFinally will be invoked before the downstream is notified of completion (return null)
				try {
					listener.doFinally(SignalType.ON_COMPLETE);
				}
				catch (Throwable listenerError) {
					throw handleObserverErrorInPoll(listenerError);
				}

				//notify the downstream of completion
				return null;
			}

			if (v != null) {
				//this is an onNext event
				try {
					listener.doOnNext(v);
				}
				catch (Throwable listenerError) {
					if (mode == SYNC) {
						throw handleObserverErrorInPoll(listenerError);
					}
					handleListenerErrorAndTerminate(listenerError);
					//TODO discard the element ?
					return null;
				}
			}
			return v;
		}

		@Override
		public int size() {
			return qs == null ? 0 : qs.size();
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		public void clear() {
			if (qs != null) {
				qs.clear();
			}
		}
	}

	static final class TapConditionalFuseableSubscriber<T> extends TapFuseableSubscriber<T> implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T> actualConditional;

		public TapConditionalFuseableSubscriber(ConditionalSubscriber<? super T> actual,
				SignalListener<T> signalListener, Context ctx) {
			super(actual, signalListener, ctx);
			this.actualConditional = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			if (actualConditional.tryOnNext(t)) {
				try {
					listener.doOnNext(t);
				}
				catch (Throwable listenerError) {
					handleListenerErrorAndTerminate(listenerError);
				}
				return true;
			}
			return false;
		}
	}
}

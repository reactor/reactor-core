/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPeek<T> extends InternalFluxOperator<T, T> implements SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	FluxPeek(Flux<? extends T> source,
			@Nullable Consumer<? super Subscription> onSubscribeCall,
			@Nullable Consumer<? super T> onNextCall,
			@Nullable Consumer<? super Throwable> onErrorCall,
			@Nullable Runnable onCompleteCall,
			@Nullable Runnable onAfterTerminateCall,
			@Nullable LongConsumer onRequestCall,
			@Nullable Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") // javac, give reason to suppress because inference anomalies
					ConditionalSubscriber<T> s2 = (ConditionalSubscriber<T>) actual;
			return new PeekConditionalSubscriber<>(s2, this);
		}
		return new PeekSubscriber<>(actual, this);
	}

	static final class PeekSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final SignalPeek<T> parent;

		Subscription s;

		boolean done;

		PeekSubscriber(CoreSubscriber<? super T> actual, SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Context currentContext() {
			Context c = actual.currentContext();
			if(!c.isEmpty() && parent.onCurrentContextCall() != null) {
				parent.onCurrentContextCall().accept(c);
			}
			return c;
		}

		@Override
		public void request(long n) {
			final LongConsumer requestHook = parent.onRequestCall();
			if (requestHook != null) {
				try {
					requestHook.accept(n);
				}
				catch (Throwable e) {
					Operators.onOperatorError(e, actual.currentContext());
				}
			}
			s.request(n);
		}

		@Override
		public void cancel() {
			final Runnable cancelHook = parent.onCancelCall();
			if (cancelHook != null) {
				try {
					cancelHook.run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, actual.currentContext()));
					return;
				}
			}
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(Operators.validate(this.s, s)) {
				final Consumer<? super Subscription> subscribeHook = parent.onSubscribeCall();
				if (subscribeHook != null) {
					try {
						subscribeHook.accept(s);
					}
					catch (Throwable e) {
						Operators.error(actual, Operators.onOperatorError(s, e,
								actual.currentContext()));
						return;
					}
				}
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if(nextHook != null) {
				try {
					nextHook.accept(t);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ == null) {
						request(1);
						return;
					}
					else {
						onError(e_);
						return;
					}
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			final Consumer<? super Throwable> errorHook = parent.onErrorCall();
			if(errorHook != null) {
				try {
					errorHook.accept(t);
				}
				catch (Throwable e) {
					//this performs a throwIfFatal or suppresses t in e
					t = Operators.onOperatorError(null, e, t, actual.currentContext());
				}
			}

			try {
				actual.onError(t);
			}
			catch (UnsupportedOperationException use){
				if(errorHook == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t){
					throw use;
				}
				//ignore if missing callback
			}

			final Runnable afterTerminateHook = parent.onAfterTerminateCall();
			if(afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					afterErrorWithFailure(parent, e, t, actual.currentContext());
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			final Runnable completeHook = parent.onCompleteCall();
			if(completeHook != null) {
				try {
					completeHook.run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, actual.currentContext()));
					return;
				}
			}
			done = true;

			actual.onComplete();

			final Runnable afterTerminateHook = parent.onAfterTerminateCall();
			if(afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					afterCompleteWithFailure(parent, e, actual.currentContext());
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}

	@Override
	@Nullable
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	@Nullable
	public Consumer<? super T> onNextCall() {
		return onNextCall;
	}

	@Override
	@Nullable
	public Consumer<? super Throwable> onErrorCall() {
		return onErrorCall;
	}

	@Override
	@Nullable
	public Runnable onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	@Nullable
	public Runnable onAfterTerminateCall() {
		return onAfterTerminateCall;
	}

	@Override
	@Nullable
	public LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	@Nullable
	public Runnable onCancelCall() {
		return onCancelCall;
	}

	/**
	 * Common method for FluxPeek and FluxPeekFuseable to deal with a doAfterTerminate
	 * callback that fails during onComplete. It drops the error to the global hook.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Throwable, Context)} is called</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable, Context)} is called</li>
	 * </ul>
	 * <p>
	 *
	 * @param parent the {@link SignalPeek} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 * @param context subscriber context
	 */
	//see https://github.com/reactor/reactor-core/issues/270
	static <T> void afterCompleteWithFailure(SignalPeek<T> parent,
			Throwable callbackFailure, Context context) {

		Exceptions.throwIfFatal(callbackFailure);
		Throwable _e = Operators.onOperatorError(callbackFailure, context);
		Operators.onErrorDropped(_e, context);
	}

	/**
	 * Common method for FluxPeek and FluxPeekFuseable to deal with a doAfterTerminate
	 * callback that fails during onError. It drops the error to the global hook.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Subscription, Throwable, Object, Context)} is
	 *     called, adding the original error as suppressed</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable, Context)} is called</li>
	 * </ul>
	 * <p>
	 *
	 * @param parent the {@link SignalPeek} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 * @param originalError the onError throwable
	 * @param context subscriber context
	 */
	//see https://github.com/reactor/reactor-core/issues/270
	static <T> void afterErrorWithFailure(SignalPeek<T> parent,
			Throwable callbackFailure, Throwable originalError, Context context) {
		Exceptions.throwIfFatal(callbackFailure);
		Throwable _e = Operators.onOperatorError(null, callbackFailure, originalError, context);
		Operators.onErrorDropped(_e, context);
	}

}
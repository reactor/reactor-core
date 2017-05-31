/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;



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
final class FluxPeek<T> extends FluxSource<T, T> implements SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	FluxPeek(Flux<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
			Consumer<? super T> onNextCall,
			Consumer<? super Throwable> onErrorCall,
			Runnable onCompleteCall,
			Runnable onAfterTerminateCall,
			LongConsumer onRequestCall,
			Runnable onCancelCall) {
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
	public void subscribe(Subscriber<? super T> s) {
		if (s instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") // javac, give reason to suppress because inference anomalies
					ConditionalSubscriber<T> s2 = (ConditionalSubscriber<T>) s;
			source.subscribe(new PeekConditionalSubscriber<>(s2, this));
			return;
		}
		source.subscribe(new PeekSubscriber<>(s, this));
	}

	static final class PeekSubscriber<T> implements InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		final SignalPeek<T> parent;

		Subscription s;

		boolean done;

		PeekSubscriber(Subscriber<? super T> actual, SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if(parent.onRequestCall() != null) {
				try {
					parent.onRequestCall().accept(n);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			s.request(n);
		}

		@Override
		public void cancel() {
			if(parent.onCancelCall() != null) {
				try {
					parent.onCancelCall().run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(Operators.validate(this.s, s)) {
				if (parent.onSubscribeCall() != null) {
					try {
						parent.onSubscribeCall()
						      .accept(s);
					}
					catch (Throwable e) {
						Operators.error(actual, Operators.onOperatorError(s, e));
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
				Operators.onNextDropped(t);
				return;
			}
			if(parent.onNextCall() != null) {
				try {
					parent.onNextCall().accept(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			if(parent.onErrorCall() != null) {
				try {
					parent.onErrorCall().accept(t);
				}
				catch (Throwable e) {
					//this performs a throwIfFatal or suppresses t in e
					t = Operators.onOperatorError(null, e, t);
				}
			}

			try {
				actual.onError(t);
			}
			catch (UnsupportedOperationException use){
				if(parent.onErrorCall() == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t){
					throw use;
				}
				//ignore if missing callback
			}

			if(parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall().run();
				}
				catch (Throwable e) {
					afterErrorWithFailure(parent, e, t);
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			if(parent.onCompleteCall() != null) {
				try {
					parent.onCompleteCall().run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			done = true;

			actual.onComplete();

			if(parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall().run();
				}
				catch (Throwable e) {
					afterCompleteWithFailure(parent, e);
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

	}

	@Override
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	public Consumer<? super T> onNextCall() {
		return onNextCall;
	}

	@Override
	public Consumer<? super Throwable> onErrorCall() {
		return onErrorCall;
	}

	@Override
	public Runnable onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	public Runnable onAfterTerminateCall() {
		return onAfterTerminateCall;
	}

	@Override
	public LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	public Runnable onCancelCall() {
		return onCancelCall;
	}

	/**
	 * Common method for FluxPeek and FluxPeekFuseable to deal with a doAfterTerminate
	 * callback that fails during onComplete. It drops the error to the global hook.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Throwable)} is called</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable)} is called</li>
	 * </ul>
	 * <p>
	 *
	 * @param parent the {@link SignalPeek} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 */
	//see https://github.com/reactor/reactor-core/issues/270
	static <T> void afterCompleteWithFailure(SignalPeek<T> parent,
			Throwable callbackFailure) {

		Exceptions.throwIfFatal(callbackFailure);
		Throwable _e = Operators.onOperatorError(callbackFailure);
		Operators.onErrorDropped(_e);
	}

	/**
	 * Common method for FluxPeek and FluxPeekFuseable to deal with a doAfterTerminate
	 * callback that fails during onError. It drops the error to the global hook.
	 * <ul>
	 *     <li>The callback failure is thrown immediately if fatal.</li>
	 *     <li>{@link Operators#onOperatorError(Subscription, Throwable, Object)} is
	 *     called, adding the original error as suppressed</li>
	 *     <li>{@link Operators#onErrorDropped(Throwable)} is called</li>
	 * </ul>
	 * <p>
	 *
	 * @param parent the {@link SignalPeek} from which to get the callbacks
	 * @param callbackFailure the afterTerminate callback failure
	 * @param originalError the onError throwable
	 */
	//see https://github.com/reactor/reactor-core/issues/270
	static <T> void afterErrorWithFailure(SignalPeek<T> parent,
			Throwable callbackFailure, Throwable originalError) {
		Exceptions.throwIfFatal(callbackFailure);
		Throwable _e = Operators.onOperatorError(null, callbackFailure, originalError);
		Operators.onErrorDropped(_e);
	}

}
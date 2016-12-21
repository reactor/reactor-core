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
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;

/**
 * Peeks the value of a {@link Mono} and execute terminal callbacks accordingly, allowing
 * to distinguish between cases where the Mono was empty, valued or errored.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Simon Baslé
 */
final class MonoPeekTerminal<T> extends MonoSource<T, T>
		implements Fuseable {

	final BiConsumer<? super T, Throwable> onAfterTerminateCall;
	final BiConsumer<? super T, Throwable> onTerminateCall;
	final Consumer<? super T>              onSuccessCall;

	public MonoPeekTerminal(Mono<? extends T> source,
			Consumer<? super T> onSuccessCall,
			BiConsumer<? super T, Throwable> onTerminateCall,
			BiConsumer<? super T, Throwable> onAfterTerminateCall) {
		super(source);
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onTerminateCall = onTerminateCall;
		this.onSuccessCall = onSuccessCall;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new MonoTerminalPeekSubscriber<>((ConditionalSubscriber<?
					super T>) s, this));
			return;
		}
		source.subscribe(new MonoTerminalPeekSubscriber<>(s, this));
	}

	/*
	The specificity of this operator's subscriber is that it is implemented as a single
	class for all cases (fuseable or not, conditional or not). So subscription and actual
	are duplicated to arrange for the special cases (QueueSubscription and ConditionalSubscriber).

	A challenge for Fuseable: classes that rely only on `instanceof Fuseable` will always
	think this operator is Fuseable, when they should also check `requestFusion`. This is
	the case with StepVerifier in 3.0.3 for instance, but actual operators should otherwise
	also call requestFusion, which will return NONE if the source isn't Fuseable.

	A challenge for ConditionalSubscriber: since there is no `requestConditional` here,
	the operators only rely on `instanceof`... So this subscriber will always seem conditional.
	As a result, if the `tryOnNext` method is invoked while the `actualConditional` is null,
	it falls back to calling `onNext` directly.
	 */
	static final class MonoTerminalPeekSubscriber<T>
			implements ConditionalSubscriber<T>, Receiver, Producer,
			           Fuseable.SynchronousSubscription<T> {

		final Subscriber<? super T> actual;
		final ConditionalSubscriber<? super T> actualConditional;

		final MonoPeekTerminal<T> parent;

		//TODO could go into a common base for all-in-one subscribers? (as well as actual above)
		Subscription s;
		Fuseable.QueueSubscription<T> queueSubscription;

		int sourceMode;

		volatile boolean done;

		/* `valued` serves as a guard against re-executing the callbacks in onComplete/onError
		as soon as onNext has been called. So onNext will set the flag immediately, then
		onNext/poll will trigger the "valued" version of ALL the callbacks (respectively
		in NONE mode and SYNC/ASYNC mode). If empty, onCompleted is called without valued
		being set, so it will execute the "empty" version of ALL callbacks. Same for onError.

		Having this flag also prevents callbacks to be attempted twice in the case of a
		callback failure, which is forwarded to onError if it happens during onNext...
		 */
		boolean valued;

		MonoTerminalPeekSubscriber(ConditionalSubscriber<? super T> actual, MonoPeekTerminal<T> parent) {
			this.actualConditional = actual;
			this.actual = null;
			this.parent = parent;
		}

		MonoTerminalPeekSubscriber(Subscriber<? super T> actual, MonoPeekTerminal<T> parent) {
			this.actual = actual;
			this.actualConditional = null;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			this.queueSubscription = Operators.as(s); //will set it to null if not Fuseable

			if (actualConditional != null) {
				actualConditional.onSubscribe(this);
			}
			else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			//implementation note: this operator doesn't expect the source to be anything but a Mono
			//so it doesn't check that valued has been set before

			valued = true;
			if (sourceMode == ASYNC) {
				if (actualConditional != null) {
					actualConditional.onNext(null);
				}
				else {
					actual.onNext(null);
				}
			}
			else if (sourceMode == NONE) {
				if (parent.onTerminateCall != null) {
					try {
						parent.onTerminateCall.accept(t, null);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, t));
						return;
					}
				}
				if (parent.onSuccessCall != null) {
					try {
						parent.onSuccessCall.accept(t);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, t));
						return;
					}
				}

				if (actualConditional != null) {
					actualConditional.onNext(t);
				}
				else {
					actual.onNext(t);
				}

				if (parent.onAfterTerminateCall != null) {
					try {
						parent.onAfterTerminateCall.accept(t, null);
					}
					catch (Throwable e) {
						//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
						Operators.onErrorDropped(Operators.onOperatorError(s, e, t));
					}
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return false;
			}
			if (actualConditional == null) {
				onNext(t); //this is the fallback if the actual isn't actually conditional
				return false;
			}

			if (sourceMode == NONE) {
				return actualConditional.tryOnNext(t);
			}
			else if (sourceMode == ASYNC) {
				actualConditional.onNext(null);
			}
			return true;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;

			if (!valued && parent.onTerminateCall != null) {
				try {
					parent.onTerminateCall.accept(null, t);
				}
				catch (Throwable e) {
					t = Operators.onOperatorError(null, e, t);
				}
			}

			try {
				if (actualConditional != null) {
					actualConditional.onError(t);
				}
				else {
					actual.onError(t);
				}
			}
			catch (UnsupportedOperationException use) {
				if (!Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			if (!valued && parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.accept(null, t);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Operators.onErrorDropped(Operators.onOperatorError(e));
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			if (!valued) {
				if (parent.onTerminateCall != null) {
					try {
						parent.onTerminateCall.accept(null, null);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e));
						return;
					}
				}
				if (parent.onSuccessCall != null) {
					try {
						parent.onSuccessCall.accept(null);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e));
						return;
					}
				}
			}

			if (actualConditional != null) {
				actualConditional.onComplete();
			}
			else {
				actual.onComplete();
			}

			if (!valued && parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.accept(null, null);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Operators.onErrorDropped(Operators.onOperatorError(e));
				}
			}
		}

		@Override
		public Object downstream() {
			return actualConditional != null ? actualConditional : actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public T poll() {
			if (queueSubscription == null) {
				//should never happen but this one is defensive
				throw new IllegalStateException("poll called without a queueSubscription");
			}
			T v = queueSubscription.poll();
			if (v != null) { //poll only called when fusion mode is either SYNC or ASYNC
				if (parent.onTerminateCall != null) {
					try {
						parent.onTerminateCall.accept(v, null);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, v));
						return null;
					}
				}
				if (parent.onSuccessCall != null) {
					try {
						parent.onSuccessCall.accept(v);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, v));
						return null;
					}
				}
				if (parent.onAfterTerminateCall != null) {
					try {
						parent.onAfterTerminateCall.accept(v, null);
					}
					catch (Throwable t) {
						Operators.onErrorDropped(Operators.onOperatorError(t));
					}
				}
			}
			return v;
		}

		@Override
		public boolean isEmpty() {
			return queueSubscription == null || queueSubscription.isEmpty();
		}

		@Override
		public void clear() {
			if (queueSubscription != null) {
				queueSubscription.clear();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if (queueSubscription == null) { //source wasn't actually Fuseable
				m = NONE;
			}
			else if ((requestedMode & THREAD_BARRIER) != 0) {
				m = NONE;
			}
			else {
				m = queueSubscription.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return queueSubscription == null ? 0 : queueSubscription.size();
		}
	}
}


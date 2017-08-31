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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;

/**
 * Peeks the value of a {@link Mono} and execute terminal callbacks accordingly, allowing
 * to distinguish between cases where the Mono was empty, valued or errored.
 *
 * @param <T> the value type
 *
 * @author Simon Baslé
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPeekTerminal<T> extends MonoOperator<T, T> implements Fuseable {

	final BiConsumer<? super T, Throwable> onAfterTerminateCall;
	final BiConsumer<? super T, Throwable> onTerminateCall;
	final Consumer<? super T>              onSuccessCall;

	MonoPeekTerminal(Mono<? extends T> source,
			@Nullable Consumer<? super T> onSuccessCall,
			@Nullable BiConsumer<? super T, Throwable> onTerminateCall,
			@Nullable BiConsumer<? super T, Throwable> onAfterTerminateCall) {
		super(source);
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onTerminateCall = onTerminateCall;
		this.onSuccessCall = onSuccessCall;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			source.subscribe(new MonoTerminalPeekSubscriber<>((ConditionalSubscriber<? super T>) actual,
					this));
			return;
		}
		source.subscribe(new MonoTerminalPeekSubscriber<>(actual, this));
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
			implements ConditionalSubscriber<T>, InnerOperator<T, T>,
			           Fuseable.QueueSubscription<T> {

		final CoreSubscriber<? super T>        actual;
		final ConditionalSubscriber<? super T> actualConditional;

		final MonoPeekTerminal<T> parent;

		//TODO could go into a common base for all-in-one subscribers? (as well as actual above)
		Subscription                  s;
		Fuseable.QueueSubscription<T> queueSubscription;

		int sourceMode;

		volatile boolean done;

		/* `valued` serves as a guard against re-executing the callbacks in onComplete/onError
		as soon as onNext has been called. So onNext will push the flag immediately, then
		onNext/poll will trigger the "valued" version of ALL the callbacks (respectively
		in NONE mode and SYNC/ASYNC mode). If empty, onCompleted is called without valued
		being push, so it will execute the "empty" version of ALL callbacks. Same for onError.

		Having this flag also prevents callbacks to be attempted twice in the case of a
		callback failure, which is forwarded to onError if it happens during onNext...
		 */ boolean valued;

		MonoTerminalPeekSubscriber(ConditionalSubscriber<? super T> actual,
				MonoPeekTerminal<T> parent) {
			this.actualConditional = actual;
			this.actual = actual;
			this.parent = parent;
		}

		MonoTerminalPeekSubscriber(CoreSubscriber<? super T> actual,
				MonoPeekTerminal<T> parent) {
			this.actual = actual;
			this.actualConditional = null;
			this.parent = parent;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
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
			this.queueSubscription =
					Operators.as(s); //will push it to null if not Fuseable

			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {

			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}
				//implementation note: this operator doesn't expect the source to be anything but a Mono
				//so it doesn't check that valued has been push before
				valued = true;

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

				actual.onNext(t);

				if (parent.onAfterTerminateCall != null) {
					try {
						parent.onAfterTerminateCall.accept(t, null);
					}
					catch (Throwable e) {
						//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
						Operators.onErrorDropped(Operators.onOperatorError(s, e, t),
								actual.currentContext());
					}
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return false;
			}
			if (actualConditional == null) {
				onNext(t); //this is the fallback if the actual isn't actually conditional
				return false;
			}

			//implementation note: this operator doesn't expect the source to be anything but a Mono
			//so it doesn't check that valued has been push before
			valued = true;

			if (parent.onTerminateCall != null) {
				try {
					parent.onTerminateCall.accept(t, null);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return false;
				}
			}
			if (parent.onSuccessCall != null) {
				try {
					parent.onSuccessCall.accept(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return false;
				}
			}

			boolean r = actualConditional.tryOnNext(t);

			if (parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.accept(t, null);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Operators.onErrorDropped(Operators.onOperatorError(s, e, t),
							actual.currentContext());
				}
			}

			return r;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
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
				actual.onError(t);
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
					Operators.onErrorDropped(Operators.onOperatorError(e),
							actual.currentContext());
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			if (sourceMode == NONE && !valued) {
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
			done = true;

			actual.onComplete();

			if (sourceMode == NONE && !valued && parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.accept(null, null);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Operators.onErrorDropped(Operators.onOperatorError(e),
							actual.currentContext());
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public T poll() {
			boolean d = done;
			T v = queueSubscription.poll();
			if (!valued && (v != null || d || sourceMode == SYNC)) {
				valued = true;
				if (parent.onTerminateCall != null) {
					try {
						parent.onTerminateCall.accept(v, null);
					}
					catch (Throwable e) {
						throw Exceptions.propagate(Operators.onOperatorError(s, e, v));
					}
				}
				if (parent.onSuccessCall != null) {
					try {
						parent.onSuccessCall.accept(v);
					}
					catch (Throwable e) {
						throw Exceptions.propagate(Operators.onOperatorError(s, e, v));
					}
				}
				if (parent.onAfterTerminateCall != null) {
					try {
						parent.onAfterTerminateCall.accept(v, null);
					}
					catch (Throwable t) {
						Operators.onErrorDropped(Operators.onOperatorError(t),
								actual.currentContext());
					}
				}
			}
			return v;
		}

		@Override
		public boolean isEmpty() {
			return queueSubscription.isEmpty();
		}

		@Override
		public void clear() {
			queueSubscription.clear();
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


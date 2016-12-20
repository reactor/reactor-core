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

	final boolean                          isFuseableSource;
	final BiConsumer<? super T, Throwable> onAfterTerminateCall;
	final BiConsumer<? super T, Throwable> onTerminateCall;
	final Consumer<? super T>              onSuccessCall;

	public MonoPeekTerminal(Publisher<? extends T> source,
			Consumer<? super T> onSuccessCall,
			BiConsumer<? super T, Throwable> onTerminateCall,
			BiConsumer<? super T, Throwable> onAfterTerminateCall) {
		super(source);
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onTerminateCall = onTerminateCall;
		this.onSuccessCall = onSuccessCall;
		this.isFuseableSource = source instanceof Fuseable;
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

	static final class MonoTerminalPeekSubscriber<T>
			implements ConditionalSubscriber<T>, Receiver, Producer,
			           Fuseable.SynchronousSubscription<T> {

		final Subscriber<? super T> actual;
		final ConditionalSubscriber<? super T> actualConditional;

		final MonoPeekTerminal<T> parent;

		Subscription s;
		Fuseable.QueueSubscription<T> queueSubscription;

		int sourceMode;

		volatile boolean done;

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
			if (parent.isFuseableSource) {
				this.queueSubscription = (Fuseable.QueueSubscription<T>) s;
			}
			else {
				this.queueSubscription = null;
			}

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
			valued = true;
			if (sourceMode == NONE) {
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
			else if (sourceMode == ASYNC) {
				if (actualConditional != null) {
					actualConditional.onNext(null);
				}
				else {
					actual.onNext(null);
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
				onError(Operators.onOperatorError(new IllegalStateException("tryOnNext called without an actualConditional")));
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
//			if (sourceMode == NONE && !valued) {
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

//			if (sourceMode == NONE && !valued) {
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
				return null; //TODO would it be best to throw or onError here?
			}
			T v = queueSubscription.poll();
			if (sourceMode == ASYNC && v != null) {
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
			if (queueSubscription == null) {
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


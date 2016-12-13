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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Producer;
import reactor.core.Receiver;

/**
 * Peek into the lifecycle events and signals of a sequence, passing around
 * a per-subscription state object initialized by a {@link Supplier} {@code stateSeeder}.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 * @param <S> the state type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPeekStateful<T, S> extends FluxSource<T, T> implements SignalPeekStateful<T, S> {

	final Supplier<S> stateSeeder;

	final BiConsumer<? super Subscription, S> onSubscribeCall;

	final BiConsumer<? super T, S> onNextCall;

	final BiConsumer<? super Throwable, S> onErrorCall;

	final Consumer<S> onCompleteCall;

	final Consumer<S> onAfterTerminateCall;

	final BiConsumer<Long, S> onRequestCall;

	final Consumer<S> onCancelCall;

	public FluxPeekStateful(Publisher<? extends T> source,
			Supplier<S> stateSeeder,
			BiConsumer<? super Subscription, S> onSubscribeCall,
			BiConsumer<? super T, S> onNextCall,
			BiConsumer<? super Throwable, S> onErrorCall,
			Consumer<S> onCompleteCall,
			Consumer<S> onAfterTerminateCall,
			BiConsumer<Long, S> onRequestCall,
			Consumer<S> onCancelCall) {
		super(source);
		this.stateSeeder = stateSeeder;
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
		//TODO fuseable version?
		//TODO conditional version?
		source.subscribe(new PeekStatefulSubscriber<>(s, this, stateSeeder.get()));
	}

	static final class PeekStatefulSubscriber<T, S> implements Subscriber<T>, Subscription, Receiver, Producer {

		final Subscriber<? super T> actual;

		final SignalPeekStateful<T, S> parent;

		final S state;

		Subscription s;

		boolean done;

		public PeekStatefulSubscriber(Subscriber<? super T> actual,
				SignalPeekStateful<T, S> parent, S state) {
			this.actual = actual;
			this.parent = parent;
			this.state = state;
		}

		@Override
		public void request(long n) {
			if(parent.onRequestCall() != null) {
				try {
					parent.onRequestCall().accept(n, state);
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
					parent.onCancelCall().accept(state);
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
			if(parent.onSubscribeCall() != null) {
				try {
					parent.onSubscribeCall().accept(s, state);
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(s, e));
					return;
				}
			}
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if(parent.onNextCall() != null) {
				try {
					parent.onNextCall().accept(t, state);
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
					parent.onErrorCall().accept(t, state);
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
					parent.onAfterTerminateCall().accept(state);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Exceptions.throwIfFatal(e);
					Throwable _e = Operators.onOperatorError(null, e, t);
					Operators.onErrorDropped(_e);
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
					parent.onCompleteCall().accept(state);
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
					parent.onAfterTerminateCall().accept(state);
				}
				catch (Throwable e) {
					//don't invoke error callback, see https://github.com/reactor/reactor-core/issues/270
					Exceptions.throwIfFatal(e);
					Throwable _e = Operators.onOperatorError(e);
					Operators.onErrorDropped(_e);
				}
			}
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}

	@Override
	public BiConsumer<? super Subscription, S> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	public BiConsumer<? super T, S> onNextCall() {
		return onNextCall;
	}

	@Override
	public BiConsumer<? super Throwable, S> onErrorCall() {
		return onErrorCall;
	}

	@Override
	public Consumer<S> onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	public Consumer<S> onAfterTerminateCall() {
		return onAfterTerminateCall;
	}

	@Override
	public BiConsumer<Long, S> onRequestCall() {
		return onRequestCall;
	}

	@Override
	public Consumer<S> onCancelCall() {
		return onCancelCall;
	}
}
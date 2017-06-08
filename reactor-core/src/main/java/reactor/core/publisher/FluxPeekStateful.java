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
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import javax.annotation.Nullable;
import reactor.util.context.Context;

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
final class FluxPeekStateful<T, S> extends FluxOperator<T, T>
		implements SignalPeekStateful<T, S> {

	final Supplier<S> stateSeeder;

	final BiConsumer<? super Subscription, S> onSubscribeCall;

	final BiConsumer<? super T, S> onNextCall;

	final BiConsumer<? super Throwable, S> onErrorCall;

	final Consumer<S> onCompleteCall;

	final Consumer<S> onAfterTerminateCall;

	final BiConsumer<Long, S> onRequestCall;

	final Consumer<S> onCancelCall;

	FluxPeekStateful(Flux<? extends T> source,
			Supplier<S> stateSeeder,
			@Nullable BiConsumer<? super Subscription, S> onSubscribeCall,
			@Nullable BiConsumer<? super T, S> onNextCall,
			@Nullable BiConsumer<? super Throwable, S> onErrorCall,
			@Nullable Consumer<S> onCompleteCall,
			@Nullable Consumer<S> onAfterTerminateCall,
			@Nullable BiConsumer<Long, S> onRequestCall,
			@Nullable Consumer<S> onCancelCall) {
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
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		//TODO fuseable version?
		//TODO conditional version?
		source.subscribe(new PeekStatefulSubscriber<>(s, this, stateSeeder.get()), ctx);
	}

	static final class PeekStatefulSubscriber<T, S> implements InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		final SignalPeekStateful<T, S> parent;

		final S state;

		Subscription s;

		boolean done;

		PeekStatefulSubscriber(Subscriber<? super T> actual,
				SignalPeekStateful<T, S> parent, S state) {
			this.actual = actual;
			this.parent = parent;
			this.state = state;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if(parent.onRequestCall() != null) {
				try {
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
					//noinspection ConstantConditions
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
		public Subscriber<? super T> actual() {
			return actual;
		}

	}

	@Override
	@Nullable
	public BiConsumer<? super Subscription, S> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	@Nullable
	public BiConsumer<? super T, S> onNextCall() {
		return onNextCall;
	}

	@Override
	@Nullable
	public BiConsumer<? super Throwable, S> onErrorCall() {
		return onErrorCall;
	}

	@Override
	@Nullable
	public Consumer<S> onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	@Nullable
	public Consumer<S> onAfterTerminateCall() {
		return onAfterTerminateCall;
	}

	@Override
	@Nullable
	public BiConsumer<Long, S> onRequestCall() {
		return onRequestCall;
	}

	@Override
	@Nullable
	public Consumer<S> onCancelCall() {
		return onCancelCall;
	}
}
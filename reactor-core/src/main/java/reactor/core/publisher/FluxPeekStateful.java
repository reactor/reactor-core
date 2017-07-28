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
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

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

	final BiConsumer<? super T, S> onNextCall;

	final BiConsumer<? super Throwable, S> onErrorCall;

	final Consumer<S> onCompleteCall;

	FluxPeekStateful(Flux<? extends T> source,
			Supplier<S> stateSeeder,
			BiConsumer<? super T, S> onNextCall,
			BiConsumer<? super Throwable, S> onErrorCall,
			Consumer<S> onCompleteCall) {
		super(source);
		this.stateSeeder = stateSeeder;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		//TODO fuseable version?
		//TODO conditional version?
		source.subscribe(new PeekStatefulSubscriber<>(s, this, stateSeeder.get()));
	}

	static final class PeekStatefulSubscriber<T, S> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final SignalPeekStateful<T, S> parent;

		final S state;

		Subscription s;

		boolean done;

		PeekStatefulSubscriber(CoreSubscriber<? super T> actual,
				SignalPeekStateful<T, S> parent, S state) {
			this.actual = actual;
			this.parent = parent;
			this.state = state;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			actual.onSubscribe(s);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
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
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

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
}
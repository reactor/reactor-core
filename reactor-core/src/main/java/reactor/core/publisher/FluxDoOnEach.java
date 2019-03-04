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

import java.util.Objects;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Peek into the lifecycle events and signals of a sequence
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDoOnEach<T> extends FluxOperator<T, T> {

	final Consumer<? super Signal<T>> onSignal;

	FluxDoOnEach(Flux<? extends T> source, Consumer<? super Signal<T>> onSignal) {
		super(source);
		this.onSignal = Objects.requireNonNull(onSignal, "onSignal");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		//TODO fuseable version?
		//TODO conditional version?
		source.subscribe(new DoOnEachSubscriber<>(actual, onSignal, false));
	}

	static final class DoOnEachSubscriber<T> implements InnerOperator<T, T>, Signal<T> {

		private static final short STATE_FLUX_START   = (short) 0;
		private static final short STATE_MONO_START   = (short) 1;
		private static final short STATE_SKIP_HANDLER = (short) 2;
		private static final short STATE_DONE         = (short) 3;

		final CoreSubscriber<? super T>   actual;
		final Context                     cachedContext;
		final Consumer<? super Signal<T>> onSignal;

		T t;

		Subscription s;

		short state;

		DoOnEachSubscriber(CoreSubscriber<? super T> actual,
				Consumer<? super Signal<T>> onSignal,
				boolean monoFlavor) {
			this.actual = actual;
			this.cachedContext = actual.currentContext();
			this.onSignal = onSignal;
			this.state = monoFlavor ? STATE_MONO_START : STATE_FLUX_START;
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
			actual.onSubscribe(this);
		}

		@Override
		public Context currentContext() {
			return cachedContext;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.TERMINATED) {
				return state == STATE_DONE;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			if (state == STATE_DONE) {
				Operators.onNextDropped(t, cachedContext);
				return;
			}
			try {
				this.t = t;
				onSignal.accept(this);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, cachedContext));
				return;
			}

			if (state == STATE_MONO_START) {
				state = STATE_SKIP_HANDLER;
				try {
					onSignal.accept(Signal.complete(cachedContext));
				}
				catch (Throwable e) {
					state = STATE_MONO_START;
					onError(Operators.onOperatorError(s, e, cachedContext));
					return;
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (state == STATE_DONE) {
				Operators.onErrorDropped(t, cachedContext);
				return;
			}
			boolean applyHandler = state < STATE_SKIP_HANDLER;
			state = STATE_DONE;
			if (applyHandler) {
				try {
					onSignal.accept(Signal.error(t, cachedContext));
				}
				catch (Throwable e) {
					//this performs a throwIfFatal or suppresses t in e
					t = Operators.onOperatorError(null, e, t, cachedContext);
				}
			}

			try {
				actual.onError(t);
			}
			catch (UnsupportedOperationException use) {
				if (!Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
				//ignore if missing callback
			}
		}

		@Override
		public void onComplete() {
			if (state == STATE_DONE) {
				return;
			}
			short oldState = state;
			state = STATE_DONE;
			if (oldState < STATE_SKIP_HANDLER) {
				try {
					onSignal.accept(Signal.complete(cachedContext));
				}
				catch (Throwable e) {
					state = oldState;
					onError(Operators.onOperatorError(s, e, cachedContext));
					return;
				}
			}

			actual.onComplete();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Nullable
		@Override
		public Throwable getThrowable() {
			return null;
		}

		@Nullable
		@Override
		public Subscription getSubscription() {
			return null;
		}

		@Nullable
		@Override
		public T get() {
			return t;
		}

		@Override
		public Context getContext() {
			return cachedContext;
		}

		@Override
		public SignalType getType() {
			return SignalType.ON_NEXT;
		}

		@Override
		public String toString() {
			return "doOnEach_onNext(" + t + ")";
		}
	}
}
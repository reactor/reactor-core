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
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

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
		source.subscribe(new DoOnEachSubscriber<>(actual, onSignal));
	}

	static final class DoOnEachSubscriber<T> implements InnerOperator<T, T>, Signal<T> {

		final CoreSubscriber<? super T> actual;

		final Consumer<? super Signal<T>> onSignal;

		T t;

		Subscription s;

		boolean done;

		DoOnEachSubscriber(CoreSubscriber<? super T> actual,
				Consumer<? super Signal<T>> onSignal) {
			this.actual = actual;
			this.onSignal = onSignal;
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
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			try {
				this.t = t;
				//noinspection ConstantConditions
				onSignal.accept(this);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
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
			try {
				//noinspection ConstantConditions
				onSignal.accept(Signal.error(t));
			}
			catch (Throwable e) {
				//this performs a throwIfFatal or suppresses t in e
				t = Operators.onOperatorError(null, e, t, actual.currentContext());
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
			if (done) {
				return;
			}
			done = true;
			try {
				//noinspection ConstantConditions
				onSignal.accept(Signal.complete());
			}
			catch (Throwable e) {
				done = false;
				onError(Operators.onOperatorError(s, e, actual.currentContext()));
				return;
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
		public SignalType getType() {
			return SignalType.ON_NEXT;
		}
	}
}
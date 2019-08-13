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
import reactor.core.Fuseable;
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
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPeekFuseable<T> extends InternalFluxOperator<T, T>
		implements Fuseable, SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	FluxPeekFuseable(Flux<? extends T> source,
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
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new PeekFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual, this);
		}
		return new PeekFuseableSubscriber<>(actual, this);
	}

	static final class PeekFuseableSubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final SignalPeek<T> parent;

		QueueSubscription<T> s;

		int sourceMode;

		volatile boolean done;

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		PeekFuseableSubscriber(CoreSubscriber<? super T> actual,
				SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			Context c = actual.currentContext();
			final Consumer<? super Context> contextHook = parent.onCurrentContextCall();
			if(!c.isEmpty() && contextHook != null) {
				contextHook.accept(c);
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

		@SuppressWarnings("unchecked")
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
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
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

				final Consumer<? super T> nextHook = parent.onNextCall();
				if (nextHook != null) {
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
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			final Consumer<? super Throwable> errorHook = parent.onErrorCall();
			if (errorHook != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (errorHook == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			final Runnable afterTerminateHook = parent.onAfterTerminateCall();
			if (afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t, actual.currentContext());
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			if (sourceMode == ASYNC) {
				done = true;
				actual.onComplete();
			}
			else {
				final Runnable completeHook = parent.onCompleteCall();
				if (completeHook != null) {
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
				if (afterTerminateHook != null) {
					try {
						afterTerminateHook.run();
					}
					catch (Throwable e) {
						FluxPeek.afterCompleteWithFailure(parent, e, actual.currentContext());
					}
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
			T v;
			try {
				v = s.poll();
			}
			catch (Throwable e) {
				final Consumer<? super Throwable> errorHook = parent.onErrorCall();
				if (errorHook != null) {
					try {
						errorHook.accept(e);
					}
					catch (Throwable errorCallbackError) {
						throw Exceptions.propagate(Operators.onOperatorError(s, errorCallbackError, e,
								actual.currentContext()));
					}
				}
				Runnable afterTerminateHook = parent.onAfterTerminateCall();
				if (afterTerminateHook != null) {
					try {
						afterTerminateHook.run();
					}
					catch (Throwable afterTerminateCallbackError) {
						throw Exceptions.propagate(Operators.onOperatorError(s, afterTerminateCallbackError, e,
								actual.currentContext()));
					}
				}
				throw Exceptions.propagate(Operators.onOperatorError(s, e,
						actual.currentContext()));
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if (v != null && nextHook != null) {
				try {
					nextHook.accept(v);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(v, e, actual.currentContext(), s);
					if (e_ == null) {
						return poll();
					}
					else {
						throw Exceptions.propagate(e_);
					}
				}
			}
			if (v == null && (d || sourceMode == SYNC)) {
				Runnable call = parent.onCompleteCall();
				if (call != null) {
					call.run();
				}
				call = parent.onAfterTerminateCall();
				if (call != null) {
					call.run();
				}
			}
			return v;
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
		}
	}

	static final class PeekFuseableConditionalSubscriber<T>
			implements ConditionalSubscriber<T>, InnerOperator<T, T>,
			           QueueSubscription<T> {

		final ConditionalSubscriber<? super T> actual;

		final SignalPeek<T> parent;

		QueueSubscription<T> s;

		int sourceMode;

		volatile boolean done;

		PeekFuseableConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			Context c = actual.currentContext();
			final Consumer<? super Context> contextHook = parent.onCurrentContextCall();
			if(!c.isEmpty() && contextHook != null) {
				contextHook.accept(c);
			}
			return c;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
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

		@SuppressWarnings("unchecked")
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
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
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

				final Consumer<? super T> nextHook = parent.onNextCall();
				if (nextHook != null) {
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
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return false;
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if (nextHook != null) {
				try {
					nextHook.accept(t);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ == null) {
						return false;
					}
					else {
						onError(e_);
						return true;
					}
				}
			}
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			final Consumer<? super Throwable> errorHook = parent.onErrorCall();
			if (errorHook != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (errorHook == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			final Runnable afterTerminateHook = parent.onAfterTerminateCall();
			if (afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t, actual.currentContext());
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			if (sourceMode == ASYNC) {
				done = true;
				actual.onComplete();
			}
			else {
				final Runnable completeHook = parent.onCompleteCall();
				if (completeHook != null) {
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
				if (afterTerminateHook != null) {
					try {
						afterTerminateHook.run();
					}
					catch (Throwable e) {
						FluxPeek.afterCompleteWithFailure(parent, e, actual.currentContext());
					}
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
			T v;
			try {
				v = s.poll();
			}
			catch (Throwable e) {
				final Consumer<? super Throwable> errorHook = parent.onErrorCall();
				if (errorHook != null) {
					try {
						errorHook.accept(e);
					}
					catch (Throwable errorCallbackError) {
						throw Exceptions.propagate(Operators.onOperatorError(s, errorCallbackError, e,
								actual.currentContext()));
					}
				}
				Runnable afterTerminateHook = parent.onAfterTerminateCall();
				if (afterTerminateHook != null) {
					try {
						afterTerminateHook.run();
					}
					catch (Throwable afterTerminateCallbackError) {
						throw Exceptions.propagate(Operators.onOperatorError(s, afterTerminateCallbackError, e,
								actual.currentContext()));
					}
				}
				throw Exceptions.propagate(Operators.onOperatorError(s, e,
						actual.currentContext()));
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if (v != null && nextHook != null) {
				try {
					nextHook.accept(v);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(v, e, actual.currentContext(), s);
					if (e_ == null) {
						return poll();
					}
					else {
						throw Exceptions.propagate(e_);
					}
				}
			}
			if (v == null && (d || sourceMode == SYNC)) {
				Runnable call = parent.onCompleteCall();
				if (call != null) {
					call.run();
				}
				call = parent.onAfterTerminateCall();
				if (call != null) {
					call.run();
				}
			}
			return v;
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
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

	static final class PeekConditionalSubscriber<T>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final SignalPeek<T> parent;

		Subscription s;

		boolean done;

		PeekConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if (nextHook != null) {
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
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return false;
			}

			final Consumer<? super T> nextHook = parent.onNextCall();
			if (nextHook != null) {
				try {
					nextHook.accept(t);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ == null) {
						return false;
					}
					else {
						onError(e_);
						return true;
					}
				}
			}
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			final Consumer<? super Throwable> errorHook = parent.onErrorCall();
			if (errorHook != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (errorHook == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			final Runnable afterTerminateHook = parent.onAfterTerminateCall();
			if (afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t, actual.currentContext());
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			final Runnable completeHook = parent.onCompleteCall();
			if (completeHook != null) {
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
			if (afterTerminateHook != null) {
				try {
					afterTerminateHook.run();
				}
				catch (Throwable e) {
					FluxPeek.afterCompleteWithFailure(parent, e, actual.currentContext());
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}
}

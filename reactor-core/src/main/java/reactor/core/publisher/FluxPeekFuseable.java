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
import reactor.core.Fuseable;



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
final class FluxPeekFuseable<T> extends FluxSource<T, T>
		implements Fuseable, SignalPeek<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	FluxPeekFuseable(Flux<? extends T> source,
			Consumer<? super Subscription> onSubscribeCall,
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
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new PeekFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) s,
					this));
			return;
		}
		source.subscribe(new PeekFuseableSubscriber<>(s, this));
	}

	static final class PeekFuseableSubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final Subscriber<? super T> actual;

		final SignalPeek<T> parent;

		QueueSubscription<T> s;

		int sourceMode;

		volatile boolean done;

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		PeekFuseableSubscriber(Subscriber<? super T> actual,
				SignalPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (parent.onRequestCall() != null) {
				try {
					parent.onRequestCall()
					      .accept(n);
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
			if (parent.onCancelCall() != null) {
				try {
					parent.onCancelCall()
					      .run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			s.cancel();
		}

		@SuppressWarnings("unchecked")
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
					Operators.onNextDropped(t);
					return;
				}
				if (parent.onNextCall() != null) {
					try {
						parent.onNextCall()
						      .accept(t);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, t));
						return;
					}
				}
				actual.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			if (parent.onErrorCall() != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (parent.onErrorCall() == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			if (parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall()
					      .run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t);
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
				if (parent.onCompleteCall() != null) {
					try {
						parent.onCompleteCall()
						      .run();
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e));
						return;
					}
				}
				done = true;

				actual.onComplete();

				if (parent.onAfterTerminateCall() != null) {
					try {
						parent.onAfterTerminateCall()
						      .run();
					}
					catch (Throwable e) {
						FluxPeek.afterCompleteWithFailure(parent, e);
					}
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public T poll() {
			boolean d = done;
			T v = s.poll();
			if (v != null && parent.onNextCall() != null) {
				try {
					parent.onNextCall()
					      .accept(v);
				}
				catch (Throwable e) {
					throw Exceptions.propagate(Operators.onOperatorError(s, e, v));
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
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (parent.onRequestCall() != null) {
				try {
					parent.onRequestCall()
					      .accept(n);
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
			if (parent.onCancelCall() != null) {
				try {
					parent.onCancelCall()
					      .run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			s.cancel();
		}

		@SuppressWarnings("unchecked")
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
					Operators.onNextDropped(t);
					return;
				}
				if (parent.onNextCall() != null) {
					try {
						parent.onNextCall()
						      .accept(t);
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e, t));
						return;
					}
				}
				actual.onNext(t);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return false;
			}

			if (parent.onNextCall() != null) {
				try {
					parent.onNextCall()
					      .accept(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return true;
				}
			}
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			if (parent.onErrorCall() != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (parent.onErrorCall() == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			if (parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall().run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t);
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
				if (parent.onCompleteCall() != null) {
					try {
						parent.onCompleteCall()
						      .run();
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(s, e));
						return;
					}
				}
				done = true;
				actual.onComplete();

				if (parent.onAfterTerminateCall() != null) {
					try {
						parent.onAfterTerminateCall()
						      .run();
					}
					catch (Throwable e) {
						FluxPeek.afterCompleteWithFailure(parent, e);
					}
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public T poll() {
			boolean d = done;
			T v = s.poll();
			if (v != null && parent.onNextCall() != null) {
				try {
					parent.onNextCall()
					      .accept(v);
				}
				catch (Throwable e) {
					throw Exceptions.propagate(Operators.onOperatorError(s, e, v));
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
		public void request(long n) {
			if (parent.onRequestCall() != null) {
				try {
					parent.onRequestCall()
					      .accept(n);
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
			if (parent.onCancelCall() != null) {
				try {
					parent.onCancelCall()
					      .run();
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
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if (parent.onNextCall() != null) {
				try {
					parent.onNextCall()
					      .accept(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
			}
			actual.onNext(t);
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return false;
			}

			if (parent.onNextCall() != null) {
				try {
					parent.onNextCall()
					      .accept(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return true;
				}
			}
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			if (parent.onErrorCall() != null) {
				Exceptions.throwIfFatal(t);
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
			catch (UnsupportedOperationException use) {
				if (parent.onErrorCall() == null
						|| !Exceptions.isErrorCallbackNotImplemented(use) && use.getCause() != t) {
					throw use;
				}
			}

			if (parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall()
					      .run();
				}
				catch (Throwable e) {
					FluxPeek.afterErrorWithFailure(parent, e, t);
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			if (parent.onCompleteCall() != null) {
				try {
					parent.onCompleteCall()
					      .run();
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e));
					return;
				}
			}
			done = true;

			actual.onComplete();

			if (parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall()
					      .run();
				}
				catch (Throwable e) {
					FluxPeek.afterCompleteWithFailure(parent, e);
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

	}
}

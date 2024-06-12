/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.BiConsumer;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Maps the values of the source publisher one-on-one via a handler function.
 * <p>
 * This variant allows composing fuseable stages.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxHandleFuseable<T, R> extends InternalFluxOperator<T, R> implements Fuseable {

	final BiConsumer<? super T, SynchronousSink<R>> handler;

	/**
	 * Constructs a FluxMap instance with the given source and handler.
	 *
	 * @param source the source Publisher instance
	 * @param handler the handler function
	 *
	 * @throws NullPointerException if either {@code source} or {@code handler} is null.
	 */
	FluxHandleFuseable(Flux<? extends T> source,
			BiConsumer<? super T, SynchronousSink<R>> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		BiConsumer<? super T, SynchronousSink<R>> handler2 = ContextPropagationSupport.shouldRestoreThreadLocalsInSomeOperators() ?
				ContextPropagation.contextRestoreForHandle(this.handler, actual::currentContext) : this.handler;
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked")
			ConditionalSubscriber<? super R> cs = (ConditionalSubscriber<? super R>) actual;
			return new HandleFuseableConditionalSubscriber<>(cs, handler2);
		}
		return new HandleFuseableSubscriber<>(actual, handler2);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class HandleFuseableSubscriber<T, R>
			implements InnerOperator<T, R>,
			           ConditionalSubscriber<T>, QueueSubscription<R>,
			           SynchronousSink<R> {

		final CoreSubscriber<? super R>                 actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean   done;
		boolean   stop;
		Throwable error;
		R         data;

		QueueSubscription<T> s;

		int sourceMode;

		HandleFuseableSubscriber(CoreSubscriber<? super R> actual,
				BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@Override
		@Deprecated
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public ContextView contextView() {
			return actual.currentContext();
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			try {
				handler.accept(t, this);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
				if (e_ != null) {
					onError(e_);
					return true;
				}
				else {
					reset();
					return false;
				}
			}
			R v = data;
			data = null;
			if (v != null) {
				actual.onNext(v);
			}
			if (stop) {
				if (error != null) {
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						done = true; //set done because we throw or go through `actual` directly
						actual.onError(e_);
					}
					else {
						reset();
						return false;
					}
				}
				else {
					done = true; //set done because we throw or go through `actual` directly
					s.cancel();
					actual.onComplete();
				}
				return true;
			}
			return v != null;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
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
				try {
					handler.accept(t, this);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						s.request(1);
					}
					return;
				}
				R v = data;
				data = null;
				if (v != null) {
					actual.onNext(v);
				}
				if (stop) {
					if (error != null) {
						Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
						if (e_ != null) {
							done = true; //set done because we throw or go through `actual` directly
							actual.onError(e_);
						}
						else {
							reset();
							s.request(1L);
						}
					}
					else{
						done = true; //set done because we throw or go through `actual` directly
						s.cancel();
						actual.onComplete();
					}
				}
				else if (v == null) {
					s.request(1L);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			done = true;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
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
		@Nullable
		public R poll() {
			if (sourceMode == ASYNC) {
				if (done) {
					return null;
				}
				long dropped = 0L;
				for (; ; ) {
					T v = s.poll();
					R u;
					if (v != null) {
						try {
							handler.accept(v, this);
						}
						catch (Throwable error){
							RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
							if (e_ != null) {
								throw e_;
							}
							else {
								reset();
								continue;
							}
						}
						u = data;
						data = null;
						if (stop) {
							if (error != null) {
								RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
								if (e_ != null) {
									done = true; //set done because we throw or go through `actual` directly
									throw e_;
								}
								//else continue
							}
							else {
								done = true; //set done because we throw or go through `actual` directly
								s.cancel();
								actual.onComplete();
							}
							return u;
						}
						if (u != null) {
							return u;
						}
						dropped++;
					}
					else if (dropped != 0L) {
						request(dropped);
						dropped = 0L;
					}
					else {
						return null;
					}
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();
					if (v != null) {
						try {
							handler.accept(v, this);
						}
						catch (Throwable error){
							RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
							if (e_ != null) {
								throw e_;
							}
							else {
								reset();
								continue;
							}
						}
						R u = data;
						data = null;
						if (stop) {
							if (error != null) {
								RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
								if (e_ != null) {
									done = true; //set done because we throw or go through `actual` directly
									throw e_;
								}
								else {
									reset();
									continue;
								}
							}
							else {
								done = true; //set done because we throw or go through `actual` directly
								return u;
							}
						}
						if (u != null) {
							return u;
						}
					}
					else {
						return null;
					}
				}
			}
		}

		private void reset() {
			done = false;
			stop = false;
			error = null;
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

		@Override
		public void complete() {
			if (stop) {
				throw new IllegalStateException("Cannot complete after a complete or error");
			}
			stop = true;
		}

		@Override
		public void error(Throwable e) {
			if (stop) {
				throw new IllegalStateException("Cannot error after a complete or error");
			}
			error = Objects.requireNonNull(e, "error");
			stop = true;
		}

		@Override
		public void next(R o) {
			if (data != null) {
				throw new IllegalStateException("Cannot emit more than one data");
			}
			if (stop) {
				throw new IllegalStateException("Cannot emit after a complete or error");
			}
			data = Objects.requireNonNull(o, "data");
		}
	}

	static final class HandleFuseableConditionalSubscriber<T, R>
			implements ConditionalSubscriber<T>, InnerOperator<T, R>,
			           QueueSubscription<R>, SynchronousSink<R> {

		final ConditionalSubscriber<? super R>          actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean   done;
		boolean   stop;
		Throwable error;
		R         data;

		QueueSubscription<T> s;

		int sourceMode;

		HandleFuseableConditionalSubscriber(ConditionalSubscriber<? super R> actual,
				BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@Override
		@Deprecated
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public ContextView contextView() {
			return actual.currentContext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {

			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else  {
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}
				try {
					handler.accept(t, this);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						reset();
						s.request(1);
					}
					return;
				}
				R v = data;
				data = null;
				if (v != null) {
					actual.onNext(v);
				}
				if (stop) {
					if (error != null) {
						Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
						if (e_ != null) {
							done = true; //set done because we throw or go through `actual` directly
							actual.onError(e_);
						}
						else {
							reset();
							s.request(1L);
						}
					}
					else {
						done = true; //set done because we throw or go through `actual` directly
						s.cancel();
						actual.onComplete();
					}
				}
				else if (v == null) {
					s.request(1L);
				}
			}
		}

		private void reset() {
			done = false;
			stop = false;
			error = null;
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			try {
				handler.accept(t, this);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
				if (e_ != null) {
					onError(e_);
					return true;
				}
				else {
					reset();
					return false;
				}
			}
			R v = data;
			data = null;
			boolean emit = false;
			if (v != null) {
				emit = actual.tryOnNext(v);
			}
			if (stop) {
				if (error != null) {
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						done = true; //set done because we throw or go through `actual` directly
						actual.onError(e_);
					}
					else {
						reset();
						return false;
					}
				}
				else {
					done = true; //set done because we throw or go through `actual` directly
					s.cancel();
					actual.onComplete();
				}
				return true;
			}
			return emit;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			done = true;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
		}

		@Override
		public void complete() {
			if (stop) {
				throw new IllegalStateException("Cannot complete after a complete or error");
			}
			stop = true;
		}

		@Override
		public void error(Throwable e) {
			if (stop) {
				throw new IllegalStateException("Cannot error after a complete or error");
			}
			error = Objects.requireNonNull(e, "error");
			stop = true;
		}

		@Override
		public void next(R o) {
			if (data != null) {
				throw new IllegalStateException("Cannot emit more than one data");
			}
			if (stop) {
				throw new IllegalStateException("Cannot emit after a complete or error");
			}
			data = Objects.requireNonNull(o, "data");
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
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
		@Nullable
		public R poll() {
			if (sourceMode == ASYNC) {
				if (done) {
					return null;
				}
				long dropped = 0L;
				for (; ; ) {
					T v = s.poll();
					R u;
					if (v != null) {
						try {
							handler.accept(v, this);
						}
						catch (Throwable error){
							RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
							if (e_ != null) {
								throw e_;
							}
							else {
								reset();
								continue;
							}
						}
						u = data;
						data = null;
						if (stop) {
							if (error != null) {
								Throwable e_ = Operators.onNextError(v, error, actual.currentContext(), s);
								if (e_ != null) {
									done = true; //set done because we throw or go through `actual` directly
									throw Exceptions.propagate(e_);
								}
								else {
									reset();
									continue;
								}
							}
							else {
								done = true; //set done because we throw or go through `actual` directly
								s.cancel();
								actual.onComplete();
							}
							return u;
						}
						if (u != null) {
							return u;
						}
						dropped++;
					}
					else if (dropped != 0L) {
						request(dropped);
						dropped = 0L;
					}
					else {
						return null;
					}
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();
					if (v != null) {
						try {
							handler.accept(v, this);
						}
						catch (Throwable error){
							RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
							if (e_ != null) {
								throw e_;
							}
							else {
								reset();
								continue;
							}
						}
						R u = data;
						data = null;
						if (stop) {
							done = true; //set done because we throw or go through `actual` directly
							if (error != null) {
								RuntimeException e_ = Operators.onNextPollError(v, error, actual.currentContext());
								if (e_ != null) {
									throw e_;
								}
								else{
									reset();
									continue;
								}
							}
							return u;
						}
						if (u != null) {
							return u;
						}
					}
					else {
						return null;
					}
				}
			}
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
}

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

import java.util.Objects;
import java.util.function.BiConsumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Maps the values of the source publisher one-on-one via a handler function as long as the handler function result is
 * not null. If the result is a {@code null} value then the source value is filtered rather than mapped.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
final class FluxHandle<T, R> extends InternalFluxOperator<T, R> {

	final BiConsumer<? super T, SynchronousSink<R>> handler;

	FluxHandle(Flux<? extends T> source, BiConsumer<? super T, SynchronousSink<R>> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		if (actual instanceof Fuseable.ConditionalSubscriber) {
			@SuppressWarnings("unchecked")
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) actual;
			return new HandleConditionalSubscriber<>(cs, handler);
		}
		return new HandleSubscriber<>(actual, handler);
	}

	static final class HandleSubscriber<T, R>
			implements InnerOperator<T, R>,
			           Fuseable.ConditionalSubscriber<T>,
			           SynchronousSink<R> {

		final CoreSubscriber<? super R>                 actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		boolean stop;
		Throwable error;
		R data;

		Subscription s;

		HandleSubscriber(CoreSubscriber<? super R> actual,
				BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public void onNext(T t) {
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
			if(stop){
				if(error != null){
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						reset();
						s.request(1L);
					}
				}
				else {
					s.cancel();
					onComplete();
				}
			}
			else if(v == null){
				s.request(1L);
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
				return false;
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
			if(stop){
				if(error != null){
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						reset();
						return false;
					}
				}
				else {
					s.cancel();
					onComplete();
				}
				return true;
			}
			return v != null;
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
			if(data != null){
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
	}

	static final class HandleConditionalSubscriber<T, R>
			implements Fuseable.ConditionalSubscriber<T>, InnerOperator<T, R>,
			           SynchronousSink<R> {
		final Fuseable.ConditionalSubscriber<? super R> actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		boolean stop;
		Throwable error;
		R data;

		Subscription s;

		HandleConditionalSubscriber(Fuseable.ConditionalSubscriber<? super R> actual, BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
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
					error = null;
					s.request(1);
				}
				return;
			}
			R v = data;
			data = null;
			if (v != null) {
				actual.onNext(v);
			}
			if(stop){
				done = true; //set done because we go through `actual` directly
				if(error != null){
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						actual.onError(e_);
					}
					else {
						reset(); //resets done
						s.request(1L);
					}
				}
				else {
					s.cancel();
					actual.onComplete();
				}
			}
			else if(v == null){
				s.request(1L);
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
				return false;
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
			boolean emit = false;
			data = null;
			if (v != null) {
				emit = actual.tryOnNext(v);
			}
			if(stop){
				done = true; //set done because we go through `actual` directly
				if(error != null){
					Throwable e_ = Operators.onNextError(t, error, actual.currentContext(), s);
					if (e_ != null) {
						actual.onError(e_);
					}
					else {
						reset(); //resets done
						return false;
					}
				}
				else {
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
		public CoreSubscriber<? super R> actual() {
			return actual;
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
			if(data != null){
				throw new IllegalStateException("Cannot emit more than one data");
			}
			if (stop) {
				throw new IllegalStateException("Cannot emit after a complete or error");
			}
			data = Objects.requireNonNull(o, "data");
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
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

}

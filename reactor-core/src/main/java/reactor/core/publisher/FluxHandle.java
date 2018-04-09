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
import java.util.Optional;
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
final class FluxHandle<T, R> extends FluxOperator<T, R> {

	static final byte STOP_NOT_STOPPED = 0;
	static final byte STOP_WILL_TERMINATE = 1;
	static final byte STOP_INTERMEDIATE = 2;
	static final byte STOP_TERMINATE = 3;

	final BiConsumer<? super T, SynchronousSink<R>>           handler;
	@Nullable
	final BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler;

	/**
	 * @param source the source
	 * @param handler the handler function
	 * @param terminateHandler the optional terminate function that can optionally change
	 * 	 * the terminate signal)
	 */
	FluxHandle(Flux<? extends T> source, BiConsumer<? super T, SynchronousSink<R>> handler,
			@Nullable BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
		this.terminateHandler = terminateHandler;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super R> actual) {
		if (actual instanceof Fuseable.ConditionalSubscriber) {
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) actual;
			source.subscribe(new HandleConditionalSubscriber<>(cs, handler, terminateHandler));
			return;
		}
		source.subscribe(new HandleSubscriber<>(actual, handler, terminateHandler));
	}

	static final class HandleSubscriber<T, R>
			implements InnerOperator<T, R>,
			           Fuseable.ConditionalSubscriber<T>,
			           SynchronousSink<R> {

		final CoreSubscriber<? super R>                 actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;
		@Nullable
		final BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler;

		boolean done;
		byte stop;
		@Nullable Throwable error;
		@Nullable R data;

		Subscription s;

		HandleSubscriber(CoreSubscriber<? super R> actual,
				BiConsumer<? super T, SynchronousSink<R>> handler,
				@Nullable BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler) {
			this.actual = actual;
			this.handler = handler;
			this.terminateHandler = terminateHandler;
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
			if(stop == STOP_INTERMEDIATE){
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
			stop = STOP_NOT_STOPPED;
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
			if(stop == STOP_INTERMEDIATE){
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

			if (stop == STOP_NOT_STOPPED) {
				if (terminateHandler != null) {
					stop = STOP_WILL_TERMINATE;
					terminateHandler.accept(Optional.of(t), this); //might do complete or error
					if (stop == STOP_TERMINATE) {
						return;
					}
				}
				//we have either a null or no-op terminateHandler
				//we'll default to the original signal
				stop = STOP_TERMINATE; //for scanUnsafe's benefit
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			if (stop == STOP_NOT_STOPPED) {
				if (terminateHandler != null) {
					stop = STOP_WILL_TERMINATE;
					terminateHandler.accept(Optional.empty(), this); //might do complete or error
					if (stop == STOP_TERMINATE) {
						return;
					}
				}
				//we have either a null or no-op terminateHandler
				//we'll default to the original signal
				stop = STOP_TERMINATE; //for scanUnsafe's benefit
			}

			actual.onComplete();
		}

		@Override
		public SynchronousSink<R> complete() {
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot complete after a complete or error");
			}
			else if (stop == STOP_WILL_TERMINATE) {
				stop = STOP_TERMINATE;
				actual.onComplete();
			}
			else {
				stop = STOP_INTERMEDIATE;
			}
			return this;
		}

		@Override
		public SynchronousSink<R> error(Throwable e) {
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot error after a complete or error");
			}
			else if (stop == STOP_WILL_TERMINATE) {
				stop = STOP_TERMINATE;
				// store error for scanUnsafe's benefit
				error = Objects.requireNonNull(e, "error in terminateHandler");
				actual.onError(error);			}
			else {
				stop = STOP_INTERMEDIATE;
				error = Objects.requireNonNull(e, "error");
			}
			return this;
		}

		@Override
		public SynchronousSink<R> next(R o) {
			if(data != null){
				throw new IllegalStateException("Cannot emit more than one data");
			}
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot emit after a complete or error");
			}
			if (stop == STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot emit in terminateHandler");
			}
			data = Objects.requireNonNull(o, "data");
			return this;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done && (stop > STOP_WILL_TERMINATE);
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
		@Nullable
		final BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler;

		boolean done;
		byte stop;
		@Nullable Throwable error;
		@Nullable R data;

		Subscription s;

		HandleConditionalSubscriber(Fuseable.ConditionalSubscriber<? super R> actual, BiConsumer<? super T, SynchronousSink<R>> handler,
				@Nullable BiConsumer<Optional<Throwable>, SynchronousSink<R>> terminateHandler) {
			this.actual = actual;
			this.handler = handler;
			this.terminateHandler = terminateHandler;
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
			if(stop > STOP_NOT_STOPPED){
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
			stop = STOP_NOT_STOPPED;
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
			if(stop > STOP_NOT_STOPPED){
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

			if (stop == STOP_NOT_STOPPED) {
				if (terminateHandler != null) {
					stop = STOP_WILL_TERMINATE;
					terminateHandler.accept(Optional.of(t), this); //might do complete or error
					if (stop == STOP_TERMINATE) {
						return;
					}
				}
				//we have either a null or no-op terminateHandler
				//we'll default to the original signal
				stop = STOP_TERMINATE; //for scanUnsafe's benefit
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			if (stop == STOP_NOT_STOPPED) {
				if (terminateHandler != null) {
					stop = STOP_WILL_TERMINATE;
					terminateHandler.accept(Optional.empty(), this); //might do complete or error
					if (stop == STOP_TERMINATE) {
						return;
					}
				}
				//we have either a null or no-op terminateHandler
				//we'll default to the original signal
				stop = STOP_TERMINATE; //for scanUnsafe's benefit
			}

			actual.onComplete();
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public SynchronousSink<R> complete() {
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot complete after a complete or error");
			}
			else if (stop == STOP_WILL_TERMINATE) {
				stop = STOP_TERMINATE;
				actual.onComplete();
			}
			else {
				stop = STOP_INTERMEDIATE;
			}
			return this;
		}

		@Override
		public SynchronousSink<R> error(Throwable e) {
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot error after a complete or error");
			}
			else if (stop == STOP_WILL_TERMINATE) {
				stop = STOP_TERMINATE;
				// store error for scanUnsafe's benefit
				error = Objects.requireNonNull(e, "error in terminateHandler");
				actual.onError(error);
			}
			else {
				stop = STOP_INTERMEDIATE;
				error = Objects.requireNonNull(e, "error");
			}
			return this;
		}

		@Override
		public SynchronousSink<R> next(R o) {
			if(data != null){
				throw new IllegalStateException("Cannot emit more than one data");
			}
			if (stop > STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot emit after a complete or error");
			}
			if (stop == STOP_WILL_TERMINATE) {
				throw new IllegalStateException("Cannot emit in terminateHandler");
			}
			data = Objects.requireNonNull(o, "data");
			return this;
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
			if (key == Attr.TERMINATED) return done && (stop > STOP_WILL_TERMINATE);
			if (key == Attr.ERROR) return error;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

}

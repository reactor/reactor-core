/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.function.BiConsumer;

import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Maps the values of the source publisher one-on-one via a handler function as long as the handler function result is
 * not null. If the result is a {@code null} value then the source value is filtered rather than mapped.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
final class FluxHandle<T, R> extends FluxSource<T, R> {

	final BiConsumer<? super T, SynchronousSink<R>> handler;

	public FluxHandle(Publisher<? extends T> source, BiConsumer<? super T, SynchronousSink<R>> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Fuseable) {
			source.subscribe(new FluxHandleFuseable.HandleFuseableSubscriber<>(s, handler));
			return;
		}
		if (s instanceof Fuseable.ConditionalSubscriber) {
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) s;
			source.subscribe(new HandleConditionalSubscriber<>(cs, handler));
			return;
		}
		source.subscribe(new HandleSubscriber<>(s, handler));
	}

	static final class HandleSubscriber<T, R>
			implements Subscriber<T>, Receiver, Producer, Loopback, Subscription,
								 Fuseable.ConditionalSubscriber<T>, Trackable,
                       SynchronousSink<R> {
		final Subscriber<? super R>			actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		Throwable error;
		R data;

		Subscription s;

		public HandleSubscriber(Subscriber<? super R> actual, BiConsumer<? super T, SynchronousSink<R>> handler) {
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
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			try {
				handler.accept(t, this);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}
			R v = data;
			data = null;
			if (v != null) {
				actual.onNext(v);
			}
			if(done){
				s.cancel();
				if(error != null){
					actual.onError(Operators.onOperatorError(s, error, t));
					return;
				}
				actual.onComplete();
			}
			else if(v == null){
				s.request(1L);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return false;
			}


			try {
				handler.accept(t, this);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return false;
			}
			R v = data;
			data = null;
			if (v != null) {
				actual.onNext(v);
			}
			if(done){
				s.cancel();
				if(error != null){
					actual.onError(error);
					return false;
				}
				actual.onComplete();
			}
			return v != null;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
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
		public Throwable getError() {
			return error;
		}

		@Override
		public void complete() {
			done = true;
		}

		@Override
		public void error(Throwable e) {
			error = Objects.requireNonNull(e, "error");
			done = true;
		}

		@Override
		public void next(R o) {
			if(data != null){
				throw new IllegalStateException("Cannot emit more than one data");
			}
			data = Objects.requireNonNull(o, "data");
		}

		@Override
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedInput() {
			return handler;
		}

		@Override
		public Object upstream() {
			return s;
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
			implements Fuseable.ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           Subscription, Trackable, SynchronousSink<R> {
		final Fuseable.ConditionalSubscriber<? super R> actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		Throwable error;
		R data;

		Subscription s;

		public HandleConditionalSubscriber(Fuseable.ConditionalSubscriber<? super R> actual, BiConsumer<? super T, SynchronousSink<R>> handler) {
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
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			try {
				handler.accept(t, this);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}
			R v = data;
			data = null;
			if (v != null) {
				actual.onNext(v);
			}
			if(done){
				s.cancel();
				if(error != null){
					actual.onError(error);
					return;
				}
				actual.onComplete();
			}
			else if(v == null){
				s.request(1L);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return false;
			}


			try {
				handler.accept(t, this);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return false;
			}
			R v = data;
			boolean emit = false;
			data = null;
			if (v != null) {
				emit = actual.tryOnNext(v);
			}
			if(done){
				s.cancel();
				if(error != null){
					actual.onError(Operators.onOperatorError(s, error, t));
				}
				else {
					actual.onComplete();
				}
			}
			return emit;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
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
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedInput() {
			return handler;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public void complete() {
			done = true;
		}

		@Override
		public void error(Throwable e) {
			error = Operators.onOperatorError(Objects.requireNonNull(e, "error"));
			done = true;
		}

		@Override
		public void next(R o) {
			if(data != null){
				throw new IllegalStateException("Cannot emit more than one data");
			}
			data = Objects.requireNonNull(o, "data");
		}

		@Override
		public Object upstream() {
			return s;
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

}

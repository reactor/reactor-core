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
import java.util.function.Function;

import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Maps the values of the source publisher one-on-one via a handler function.
 * <p>
 * This variant allows composing fuseable stages.
 * 
 * @param <T> the source value type
 * @param <R> the result value type
 */


/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxHandleFuseable<T, R> extends FluxSource<T, R>
		implements Fuseable {

	final BiConsumer<? super T, SynchronousSink<R>> handler;

	/**
	 * Constructs a FluxMap instance with the given source and handler.
	 *
	 * @param source the source Publisher instance
	 * @param handler the handler function
	 * @throws NullPointerException if either {@code source} or {@code handler} is null.
	 */
	public FluxHandleFuseable(Publisher<? extends T> source,
			BiConsumer<? super T, SynchronousSink<R>> handler) {
		super(source);
		this.handler = Objects.requireNonNull(handler, "handler");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (s instanceof ConditionalSubscriber) {
			
			ConditionalSubscriber<? super R> cs = (ConditionalSubscriber<? super R>) s;
			source.subscribe(new HandleFuseableConditionalSubscriber<>(cs, handler));
			return;
		}
		source.subscribe(new HandleFuseableSubscriber<>(s, handler));
	}

	static final class HandleFuseableSubscriber<T, R>
			implements Subscriber<T>, Receiver, Producer, Loopback, Subscription,
			           SynchronousSubscription<R>, Trackable, SynchronousSink<R> {
		final Subscriber<? super R>			actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		Throwable error;
		R data;

		QueueSubscription<T> s;

		int sourceMode;

		public HandleFuseableSubscriber(Subscriber<? super R> actual, BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>)s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			int m = sourceMode;
			
			if (m == NONE) {
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
	
			} else
			if (m == ASYNC) {
				actual.onNext(null);
			}
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

		@Override
		public R poll() {
			if(done){
				if(error != null){
					actual.onError(error);
				}
				return null;
			}
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (;;) {
					T v = s.poll();
					R u;
					if(v != null){
						handler.accept(v, this);
						u = data;
						data = null;
						if(done){
							s.cancel();
							if(error != null){
								actual.onError(error);
							}
							return u;
						}
						if(u != null){
							return u;
						}
						dropped++;
					}
					else if (dropped != 0) {
						request(dropped);
					}
				}
			} else {
				for (;;) {
					T v = s.poll();
					if (v != null) {
						handler.accept(v, this);
						R u = data;
						data = null;
						if(done){
							if(error != null){
								actual.onError(error);
							}
							return u;
						}
						if(u != null) {
							return u;
						}
					} else {
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
				if ((requestedMode & Fuseable.SYNC) != 0) {
					m = s.requestFusion(Fuseable.SYNC);
				} else {
					m = Fuseable.NONE;
				}
			} else {
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
	}

	static final class HandleFuseableConditionalSubscriber<T, R>
			implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           SynchronousSubscription<R>, Trackable, SynchronousSink<R> {
		final ConditionalSubscriber<? super R>			actual;
		final BiConsumer<? super T, SynchronousSink<R>> handler;

		boolean done;
		Throwable error;
		R data;

		QueueSubscription<T> s;

		int sourceMode;

		public HandleFuseableConditionalSubscriber(ConditionalSubscriber<? super R>
				actual, BiConsumer<? super T, SynchronousSink<R>> handler) {
			this.actual = actual;
			this.handler = handler;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>)s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			int m = sourceMode;
			
			if (m == 0) {
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
			} else
			if (m == 2) {
				actual.onNext(null);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			int m = sourceMode;
			
			if (m == 0) {
				try {
					handler.accept(t, this);
				} catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return false;
				}
				R v = data;
				data = null;
				boolean emit = false;
				if (v != null) {
					emit = actual.tryOnNext(v);
				}
				if(done){
					s.cancel();
					if(error != null){
						actual.onError(error);
					}
					else {
						actual.onComplete();
					}
					return emit;
				}
				else if(v == null){
					return false;
				}
			} else
			if (m == 2) {
				actual.onNext(null);
			}
			return true;
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

		@Override
		public R poll() {
			if (done){
				if(error != null){
					actual.onError(error);
				}
				return null;
			}
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (;;) {
					T v = s.poll();
					R u;
					if(v != null){
						handler.accept(v, this);
						u = data;
						data = null;
						if(done){
							s.cancel();
							if(error != null){
								actual.onError(error);
							}
							else {
								actual.onComplete();
							}
							return u;
						}
						if(u != null){
							return u;
						}
						dropped++;
					}
					else if (dropped != 0) {
						request(dropped);
					}
				}
			} else {
				for (;;) {
					T v = s.poll();
					if (v != null) {
						handler.accept(v, this);
						R u = data;
						data = null;
						if(done){
							if(error != null){
								actual.onError(error);
							}
							return u;
						}
						if(u != null) {
							return u;
						}
					} else {
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
				if ((requestedMode & Fuseable.SYNC) != 0) {
					m = s.requestFusion(Fuseable.SYNC);
				} else {
					m = Fuseable.NONE;
				}
			} else {
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

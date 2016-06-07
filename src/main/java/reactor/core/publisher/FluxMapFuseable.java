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

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Completable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 * <p>
 * This variant allows composing fuseable stages.
 * 
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxMapFuseable<T, R> extends FluxSource<T, R>
		implements Fuseable {

	final Function<? super T, ? extends R> mapper;

	/**
	 * Constructs a FluxMap instance with the given source and mapper.
	 *
	 * @param source the source Publisher instance
	 * @param mapper the mapper function
	 * @throws NullPointerException if either {@code source} or {@code mapper} is null.
	 */
	public FluxMapFuseable(Publisher<? extends T> source, Function<? super T, ? extends R> mapper) {
		super(source);
		if (!(source instanceof Fuseable)) {
			throw new IllegalArgumentException("The source must implement the Fuseable interface for this operator to work");
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	public Function<? super T, ? extends R> mapper() {
		return mapper;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (s instanceof ConditionalSubscriber) {
			
			ConditionalSubscriber<? super R> cs = (ConditionalSubscriber<? super R>) s;
			source.subscribe(new MapFuseableConditionalSubscriber<>(cs, mapper));
			return;
		}
		source.subscribe(new MapFuseableSubscriber<>(s, mapper));
	}

	static final class MapFuseableSubscriber<T, R>
	implements Subscriber<T>, Completable, Receiver, Producer, Loopback, SynchronousSubscription<R> {
		final Subscriber<? super R>			actual;
		final Function<? super T, ? extends R> mapper;

		boolean done;

		QueueSubscription<T> s;

		int sourceMode;

		public MapFuseableSubscriber(Subscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = (QueueSubscription<T>)s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			int m = sourceMode;
			
			if (m == NONE) {
				R v;
	
				try {
					v = mapper.apply(t);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					s.cancel();
					onError(Exceptions.unwrap(e));
					return;
				}
	
				if (v == null) {
					s.cancel();
					onError(new NullPointerException("The mapper returned a null value."));
					return;
				}
	
				actual.onNext(v);
			} else
			if (m == ASYNC) {
				actual.onNext(null);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
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
			return mapper;
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
			T v = s.poll();
			if (v != null) {
				R u = mapper.apply(v);
				if (u == null) {
					throw new NullPointerException();
				}
				return u;
			}
			return null;
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

	static final class MapFuseableConditionalSubscriber<T, R> 
	implements ConditionalSubscriber<T>, Completable, Receiver, Producer, Loopback, SynchronousSubscription<R> {
		final ConditionalSubscriber<? super R>			actual;
		final Function<? super T, ? extends R> mapper;

		boolean done;

		QueueSubscription<T> s;

		int sourceMode;

		public MapFuseableConditionalSubscriber(ConditionalSubscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = (QueueSubscription<T>)s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			int m = sourceMode;
			
			if (m == 0) {
				R v;
	
				try {
					v = mapper.apply(t);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					s.cancel();
					onError(Exceptions.unwrap(e));
					return;
				}
	
				if (v == null) {
					done = true;
					s.cancel();
					actual.onError(new NullPointerException("The mapper returned a null value."));
					return;
				}
	
				actual.onNext(v);
			} else
			if (m == 2) {
				actual.onNext(null);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return true;
			}

			int m = sourceMode;
			
			if (m == 0) {
				R v;
	
				try {
					v = mapper.apply(t);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					s.cancel();
					onError(Exceptions.unwrap(e));
					return true;
				}
	
				if (v == null) {
					done = true;
					s.cancel();
					actual.onError(new NullPointerException("The mapper returned a null value."));
					return true;
				}
	
				return actual.tryOnNext(v);
			} else
			if (m == 2) {
				actual.onNext(null);
			}
			return true;
		}

		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
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
			return mapper;
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
			T v = s.poll();
			if (v != null) {
				R u = mapper.apply(v);
				if (u == null) {
					throw new NullPointerException();
				}
				return u;
			}
			return null;
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

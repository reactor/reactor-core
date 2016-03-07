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
import reactor.core.publisher.FluxMapFuseable.MapFuseableSubscriber;
import reactor.core.state.Completable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxMap<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends R> mapper;

	/**
	 * Constructs a StreamMap instance with the given source and mapper.
	 *
	 * @param source the source Publisher instance
	 * @param mapper the mapper function
	 * @throws NullPointerException if either {@code source} or {@code mapper} is null.
	 */
	public FluxMap(Publisher<? extends T> source, Function<? super T, ? extends R> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	public Function<? super T, ? extends R> mapper() {
		return mapper;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Fuseable) {
			source.subscribe(new MapFuseableSubscriber<>(s, mapper));
			return;
		}
		if (s instanceof Fuseable.ConditionalSubscriber) {
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) s;
			source.subscribe(new MapConditionalSubscriber<>(cs, mapper));
			return;
		}
		source.subscribe(new MapSubscriber<>(s, mapper));
	}

	static final class MapSubscriber<T, R> implements Subscriber<T>, Completable, Receiver, Producer, Loopback, Subscription {
		final Subscriber<? super R>			actual;
		final Function<? super T, ? extends R> mapper;

		boolean done;

		Subscription s;

		public MapSubscriber(Subscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			R v;

			try {
				v = mapper.apply(t);
			} catch (Throwable e) {
				done = true;
				s.cancel();
				actual.onError(e);
				return;
			}

			if (v == null) {
				done = true;
				s.cancel();
				actual.onError(new NullPointerException("The mapper returned a null value."));
				return;
			}

			actual.onNext(v);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				throw Exceptions.wrapUpstream(t);
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
	}

	static final class MapConditionalSubscriber<T, R> implements Fuseable.ConditionalSubscriber<T>, Completable, Receiver, Producer, Loopback, Subscription {
		final Fuseable.ConditionalSubscriber<? super R> actual;
		final Function<? super T, ? extends R> mapper;

		boolean done;

		Subscription s;

		public MapConditionalSubscriber(Fuseable.ConditionalSubscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			R v;

			try {
				v = mapper.apply(t);
			} catch (Throwable e) {
				done = true;
				s.cancel();
				actual.onError(e);
				return;
			}

			if (v == null) {
				done = true;
				s.cancel();
				actual.onError(new NullPointerException("The mapper returned a null value."));
				return;
			}

			actual.onNext(v);
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return true;
			}

			R v;

			try {
				v = mapper.apply(t);
			} catch (Throwable e) {
				done = true;
				s.cancel();
				actual.onError(e);
				return true;
			}

			if (v == null) {
				done = true;
				s.cancel();
				actual.onError(new NullPointerException("The mapper returned a null value."));
				return true;
			}

			return actual.tryOnNext(v);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				throw Exceptions.wrapUpstream(t);
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
	}

}

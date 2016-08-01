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
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.publisher.FluxMapFuseable.MapFuseableSubscriber;
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxMap<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends R> mapper;
	final boolean filterNullResult;

	/**
	 * Constructs a FluxMap instance with the given source and mapper.
	 *
	 * @param source the source Publisher instance
	 * @param mapper the mapper function
	 * @throws NullPointerException if either {@code source} or {@code mapper} is null.
	 */
	public FluxMap(Publisher<? extends T> source, Function<? super T, ? extends R> mapper, boolean filterNullResult) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.filterNullResult = filterNullResult;
	}

	public Function<? super T, ? extends R> mapper() {
		return mapper;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Fuseable) {
			source.subscribe(new MapFuseableSubscriber<>(s, mapper, filterNullResult));
			return;
		}
		if (s instanceof Fuseable.ConditionalSubscriber) {
			Fuseable.ConditionalSubscriber<? super R> cs = (Fuseable.ConditionalSubscriber<? super R>) s;
			source.subscribe(new MapConditionalSubscriber<>(cs, mapper));
			return;
		}
		source.subscribe(new MapSubscriber<>(s, mapper, filterNullResult));
	}

	static final class MapSubscriber<T, R>
			implements Subscriber<T>, Receiver, Producer, Loopback, Subscription,
			           Trackable {
		final Subscriber<? super R>			actual;
		final Function<? super T, ? extends R> mapper;
		final boolean filterNullResults;

		boolean done;

		Subscription s;

		public MapSubscriber(Subscriber<? super R> actual, Function<? super T, ? extends R> mapper, boolean filterNullResults) {
			this.actual = actual;
			this.mapper = mapper;
			this.filterNullResults = filterNullResults;
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
				Exceptions.onNextDropped(t);
				return;
			}

			R v;

			try {
				v = mapper.apply(t);
			} catch (Throwable e) {
				onError(Exceptions.mapOperatorError(s, e));
				return;
			}

			if (v == null) {
				if(filterNullResults) {
					done = true;
					s.cancel();
					actual.onError(new NullPointerException("The mapper returned a null value."));
					return;
				} else {
					s.request(1);
					return;
				}
			}

			actual.onNext(v);
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
	}

	static final class MapConditionalSubscriber<T, R>
			implements Fuseable.ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           Subscription, Trackable {
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
			if (Operators.validate(this.s, s)) {
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
				onError(Exceptions.mapOperatorError(s, e));
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
				onError(Exceptions.mapOperatorError(s, e));
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
	}

}

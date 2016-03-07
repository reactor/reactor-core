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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Completable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxDistinct<T, K, C extends Collection<? super K>> extends FluxSource<T, T> {

	final Function<? super T, ? extends K> keyExtractor;

	final Supplier<C> collectionSupplier;

	public FluxDistinct(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
							 Supplier<C> collectionSupplier) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		C collection;

		try {
			collection = collectionSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (collection == null) {
			EmptySubscription.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
			return;
		}

		source.subscribe(new DistinctSubscriber<>(s, collection, keyExtractor));
	}

	static final class DistinctSubscriber<T, K, C extends Collection<? super K>>
			implements Subscriber<T>, Receiver, Producer, Loopback, Completable, Subscription {
		final Subscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		Subscription s;

		boolean done;

		public DistinctSubscriber(Subscriber<? super T> actual, C collection,
										   Function<? super T, ? extends K> keyExtractor) {
			this.actual = actual;
			this.collection = collection;
			this.keyExtractor = keyExtractor;
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

			K k;

			try {
				k = keyExtractor.apply(t);
			} catch (Throwable e) {
				s.cancel();
				Exceptions.throwIfFatal(e);
				onError(Exceptions.unwrap(e));
				return;
			}

			boolean b;

			try {
				b = collection.add(k);
			} catch (Throwable e) {
				s.cancel();

				onError(e);
				return;
			}


			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
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
			return keyExtractor;
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

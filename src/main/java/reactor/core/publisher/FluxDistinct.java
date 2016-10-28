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
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Fuseable.QueueSubscription;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
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
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		C collection;

		try {
			collection = collectionSupplier.get();
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		if (collection == null) {
			Operators.error(s, Operators.onOperatorError(new
					NullPointerException("The collectionSupplier returned a null collection")));
			return;
		}

		if (source instanceof Fuseable) {
			source.subscribe(new DistinctFuseableSubscriber<>(s,
					collection,
					keyExtractor));
		}
		else if (s instanceof ConditionalSubscriber) {
			source.subscribe(new DistinctConditionalSubscriber<>((ConditionalSubscriber<? super T>) s,
					collection,
					keyExtractor));
		}
		else {
			source.subscribe(new DistinctSubscriber<>(s, collection, keyExtractor));
		}
	}

	static final class DistinctSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           Subscription, Trackable {
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
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			K k;

			try {
				k = keyExtractor.apply(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}


			if (b) {
				actual.onNext(t);
				return true;
			}
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			collection.clear();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			collection.clear();

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

	static final class DistinctConditionalSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           Subscription, Trackable {
		final ConditionalSubscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		Subscription s;

		boolean done;

		public DistinctConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				C collection,
				Function<? super T, ? extends K> keyExtractor) {
			this.actual = actual;
			this.collection = collection;
			this.keyExtractor = keyExtractor;
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

			K k;

			try {
				k = keyExtractor.apply(t);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			boolean b;

			try {
				b = collection.add(k);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}


			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			K k;

			try {
				k = keyExtractor.apply(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			return b && actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			collection.clear();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			collection.clear();

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

	static final class DistinctFuseableSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
			           QueueSubscription<T>, Trackable {
		final Subscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		QueueSubscription<T> qs;

		boolean done;

		int sourceMode;

		public DistinctFuseableSubscriber(Subscriber<? super T> actual, C collection,
				Function<? super T, ? extends K> keyExtractor) {
			this.actual = actual;
			this.collection = collection;
			this.keyExtractor = keyExtractor;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.qs, s)) {
				this.qs = (QueueSubscription<T>) s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!tryOnNext(t)) {
				qs.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			if (sourceMode == Fuseable.ASYNC) {
				actual.onNext(null);
				return true;
			}

			K k;

			try {
				k = keyExtractor.apply(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(qs, e, t));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(qs, e, t));
				return true;
			}


			if (b) {
				actual.onNext(t);
				return true;
			}
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			collection.clear();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			collection.clear();

			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return qs != null && !done;
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
			return qs;
		}

		@Override
		public void request(long n) {
			qs.request(n);
		}

		@Override
		public void cancel() {
			qs.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m = qs.requestFusion(requestedMode);
			sourceMode = m;
			return m;
		}

		@Override
		public T poll() {
			if (sourceMode == Fuseable.ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = qs.poll();

					if (v == null || collection.add(keyExtractor.apply(v))) {
						if (dropped != 0) {
							request(dropped);
						}
						return v;
					}
					dropped++;
				}
			}
			else {
				for (; ; ) {
					T v = qs.poll();

					if (v == null || collection.add(keyExtractor.apply(v))) {
						return v;
					}
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return qs.isEmpty();
		}

		@Override
		public void clear() {
			qs.clear();
			collection.clear();
		}

		@Override
		public int size() {
			return qs.size();
		}
	}

}

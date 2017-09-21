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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Fuseable.QueueSubscription;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extracted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDistinct<T, K, C extends Collection<? super K>> extends
                                                                FluxOperator<T, T> {

	final Function<? super T, ? extends K> keyExtractor;

	final Supplier<C> collectionSupplier;

	FluxDistinct(Flux<? extends T> source,
			Function<? super T, ? extends K> keyExtractor,
			Supplier<C> collectionSupplier) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier =
				Objects.requireNonNull(collectionSupplier, "collectionSupplier");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		C collection;

		try {
			collection = Objects.requireNonNull(collectionSupplier.get(),
					"The collectionSupplier returned a null collection");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		if (actual instanceof ConditionalSubscriber) {
			source.subscribe(new DistinctConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					collection,
					keyExtractor));
		}
		else {
			source.subscribe(new DistinctSubscriber<>(actual, collection, keyExtractor));
		}
	}

	static final class DistinctSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		Subscription s;

		boolean done;

		DistinctSubscriber(CoreSubscriber<? super T> actual,
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
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
				"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
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
				Operators.onErrorDropped(t, actual.currentContext());
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
		public CoreSubscriber<? super T> actual() {
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
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class DistinctConditionalSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		Subscription s;

		boolean done;

		DistinctConditionalSubscriber(ConditionalSubscriber<? super T> actual,
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
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
				"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			if (b) {
				actual.onNext(t);
			}
			else {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
						"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return true;
			}

			return b && actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
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
		public CoreSubscriber<? super T> actual() {
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
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class DistinctFuseableSubscriber<T, K, C extends Collection<? super K>>
			implements ConditionalSubscriber<T>, InnerOperator<T, T>,
			           QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		QueueSubscription<T> qs;

		boolean done;

		int sourceMode;

		DistinctFuseableSubscriber(CoreSubscriber<? super T> actual,
				C collection,
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

			if (sourceMode == Fuseable.ASYNC) {
				actual.onNext(null);
				return true;
			}
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
						"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(qs, e, t, actual.currentContext()));
				return true;
			}

			boolean b;

			try {
				b = collection.add(k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(qs, e, t, actual.currentContext()));
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
				Operators.onErrorDropped(t, actual.currentContext());
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
		public CoreSubscriber<? super T> actual() {
			return actual;
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
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return qs;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		@Nullable
		public T poll() {
			if (sourceMode == Fuseable.ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = qs.poll();
					if (v == null) {
						return null;
					}
					K r = Objects.requireNonNull(keyExtractor.apply(v),
							"The keyExtractor returned a null collection");

					if (collection.add(r)) {
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
					if (v == null) {
						return null;
					}
					K r = Objects.requireNonNull(keyExtractor.apply(v),
							"The keyExtractor returned a null collection");

					if (collection.add(r)) {
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

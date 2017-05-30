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
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;

/**
 * Buffers all values from the source Publisher and emits it as a single Collection.
 *
 * @param <T> the source value type
 * @param <C> the collection type that takes any supertype of T
 */
final class MonoCollectList<T, C extends Collection<? super T>> extends MonoSource<T, C>
		implements Fuseable {

	final Supplier<C> collectionSupplier;

	MonoCollectList(Flux<? extends T> source,
			Supplier<C> collectionSupplier) {
		super(source);
		this.collectionSupplier = collectionSupplier;
	}

	@Override
	public void subscribe(Subscriber<? super C> s) {
		C collection;

		try {
			collection = Objects.requireNonNull(collectionSupplier.get(),
					"The collectionSupplier returned a null collection");
		}
		catch (Throwable ex) {
			Operators.error(s, Operators.onOperatorError(ex));
			return;
		}

		source.subscribe(new MonoBufferAllSubscriber<>(s, collection));
	}

	static final class MonoBufferAllSubscriber<T, C extends Collection<? super T>>
			extends Operators.MonoSubscriber<T, C> {

		C collection;

		Subscription s;

		MonoBufferAllSubscriber(Subscriber<? super C> actual, C collection) {
			super(actual);
			this.collection = collection;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return collection == null;

			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			collection.add(t);
		}

		@Override
		public void onError(Throwable t) {
			C c = collection;
			if(c == null){
				return;
			}
			collection = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			C c = collection;
			if(c == null){
				return;
			}
			collection = null;

			complete(c);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

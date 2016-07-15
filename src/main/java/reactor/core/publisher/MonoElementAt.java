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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.Exceptions;

/**
 * Emits only the element at the given index position or signals a
 * default value if specified or IndexOutOfBoundsException if the sequence is shorter.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoElementAt<T> extends MonoSource<T, T> implements Fuseable {

	final long index;

	final Supplier<? extends T> defaultSupplier;

	public MonoElementAt(Publisher<? extends T> source, long index) {
		super(source);
		if (index < 0) {
			throw new IndexOutOfBoundsException("index >= required but it was " + index);
		}
		this.index = index;
		this.defaultSupplier = null;
	}

	public MonoElementAt(Publisher<? extends T> source, long index, Supplier<? extends T> defaultSupplier) {
		super(source);
		if (index < 0) {
			throw new IndexOutOfBoundsException("index >= required but it was " + index);
		}
		this.index = index;
		this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new ElementAtSubscriber<>(s, index, defaultSupplier));
	}

	static final class ElementAtSubscriber<T>
			extends Operators.DeferredScalarSubscriber<T, T>
			implements Receiver {
		final Supplier<? extends T> defaultSupplier;

		long index;

		Subscription s;

		boolean done;

		public ElementAtSubscriber(Subscriber<? super T> actual, long index,
											Supplier<? extends T> defaultSupplier) {
			super(actual);
			this.index = index;
			this.defaultSupplier = defaultSupplier;
		}

		@Override
		public void request(long n) {
			super.request(n);
			if (n > 0L) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			long i = index;
			if (i == 0) {
				done = true;
				s.cancel();

				subscriber.onNext(t);
				subscriber.onComplete();
				return;
			}
			index = i - 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;

			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			Supplier<? extends T> ds = defaultSupplier;

			if (ds == null) {
				subscriber.onError(new IndexOutOfBoundsException());
			} else {
				T t;

				try {
					t = ds.get();
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					subscriber.onError(Exceptions.unwrap(e));
					return;
				}

				if (t == null) {
					subscriber.onError(new NullPointerException("The defaultSupplier returned a null value"));
					return;
				}

				complete(t);
			}
		}


		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public Object connectedInput() {
			return defaultSupplier;
		}
	}
}

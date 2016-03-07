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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Receiver;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Expects and emits a single item from the source or signals
 * NoSuchElementException(or a default generated value) for empty source,
 * IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
final class MonoSingle<T> extends MonoSource<T, T> {

	private static final Supplier<Object> COMPLETE_ON_EMPTY_SEQUENCE = new Supplier<Object>() {
		@Override
		public Object get() {
			return null; // Purposedly leave noop
		}
	};

	/**
	 * @param <T>
	 * @return a Supplier instance marker that bypass NoSuchElementException if empty
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<T> completeOnEmptySequence() {
		return (Supplier<T>)COMPLETE_ON_EMPTY_SEQUENCE;
	}

	final Supplier<? extends T> defaultSupplier;

	public MonoSingle(Publisher<? extends T> source) {
		super(source);
		this.defaultSupplier = null;
	}

	public MonoSingle(Publisher<? extends T> source, Supplier<? extends T> defaultSupplier) {
		super(source);
		this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new SingleSubscriber<>(s, defaultSupplier));
	}

	static final class SingleSubscriber<T> extends DeferredScalarSubscriber<T, T>
			implements Receiver {

		final Supplier<? extends T> defaultSupplier;

		Subscription s;

		int count;

		boolean done;

		public SingleSubscriber(Subscriber<? super T> actual, Supplier<? extends T> defaultSupplier) {
			super(actual);
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
		public void setValue(T value) {
			this.value = value;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
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
			value = t;

			if (++count > 1) {
				cancel();

				onError(new IndexOutOfBoundsException("Source emitted more than one item"));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				throw Exceptions.wrapUpstream(t);
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

			int c = count;
			if (c == 0) {
				Supplier<? extends T> ds = defaultSupplier;
				if (ds != null) {

					if (ds == COMPLETE_ON_EMPTY_SEQUENCE){
						subscriber.onComplete();
						return;
					}

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
				} else {
					subscriber.onError(new NoSuchElementException("Source was empty"));
				}
			} else if (c == 1) {
				subscriber.onNext(value);
				subscriber.onComplete();
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
			return defaultSupplier != COMPLETE_ON_EMPTY_SEQUENCE ? defaultSupplier : null;
		}

	}
}

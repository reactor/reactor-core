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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.Exceptions;

/**
 * Collects the values of the source sequence into a container returned by
 * a supplier and a collector action working on the container and the current source
 * value.
 *
 * @param <T> the source value type
 * @param <R> the container value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoCollect<T, R> extends MonoSource<T, R> implements Fuseable {

	final Supplier<R> supplier;

	final BiConsumer<? super R, ? super T> action;

	public MonoCollect(Publisher<? extends T> source, Supplier<R> supplier,
							BiConsumer<? super R, ? super T> action) {
		super(source);
		this.supplier = Objects.requireNonNull(supplier, "supplier");
		this.action = Objects.requireNonNull(action);
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		R container;

		try {
			container = supplier.get();
		} catch (Throwable e) {
			Operators.error(s, e);
			return;
		}

		if (container == null) {
			Operators.error(s, new NullPointerException("The supplier returned a null container"));
			return;
		}

		source.subscribe(new CollectSubscriber<>(s, action, container));
	}

	static final class CollectSubscriber<T, R>
			extends Operators.DeferredScalarSubscriber<T, R>
			implements Receiver {

		final BiConsumer<? super R, ? super T> action;

		Subscription s;

		boolean done;

		public CollectSubscriber(Subscriber<? super R> actual, BiConsumer<? super R, ? super T> action,
										  R container) {
			super(actual);
			this.action = action;
			this.value = container;
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			try {
				action.accept(value, t);
			} catch (Throwable e) {
				cancel();
				Exceptions.throwIfFatal(e);
				onError(Exceptions.unwrap(e));
			}
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
			complete(value);
		}

		@Override
		public void setValue(R value) {
			// value is constant
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
			return action;
		}

		@Override
		public Object connectedOutput() {
			return value;
		}
	}
}

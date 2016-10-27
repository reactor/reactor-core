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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.MultiReceiver;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxConcatIterable<T> extends Flux<T>
		implements MultiReceiver {

	final Iterable<? extends Publisher<? extends T>> iterable;

	public FluxConcatIterable(Iterable<? extends Publisher<? extends T>> iterable) {
		this.iterable = Objects.requireNonNull(iterable, "iterable");
	}

	@Override
	public Iterator<?> upstreams() {
		return iterable.iterator();
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		Iterator<? extends Publisher<? extends T>> it;

		try {
			it = iterable.iterator();
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		if (it == null) {
			Operators.error(s, Operators.onOperatorError(new
					NullPointerException("The Iterator returned is null")));
			return;
		}

		ConcatIterableSubscriber<T> parent = new ConcatIterableSubscriber<>(s, it);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
	}

	static final class ConcatIterableSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Iterator<? extends Publisher<? extends T>> it;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatIterableSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(ConcatIterableSubscriber.class, "wip");

		long produced;

		public ConcatIterableSubscriber(Subscriber<? super T> actual, Iterator<? extends Publisher<? extends
		  T>> it) {
			super(actual);
			this.it = it;
		}

		@Override
		public void onNext(T t) {
			produced++;

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Iterator<? extends Publisher<? extends T>> a = this.it;
				do {
					if (isCancelled()) {
						return;
					}

					boolean b;

					try {
						b = a.hasNext();
					} catch (Throwable e) {
						onError(Operators.onOperatorError(this, e));
						return;
					}

					if (isCancelled()) {
						return;
					}


					if (!b) {
						subscriber.onComplete();
						return;
					}

					Publisher<? extends T> p;

					try {
						p = it.next();
					} catch (Throwable e) {
						subscriber.onError(Operators.onOperatorError(this, e));
						return;
					}

					if (isCancelled()) {
						return;
					}

					if (p == null) {
						subscriber.onError(Operators.onOperatorError(this, new
								NullPointerException("The Publisher returned by the " +
								"iterator is null")));
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

				} while (WIP.decrementAndGet(this) != 0);
			}

		}
	}
}

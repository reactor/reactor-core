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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

/**
 * Delays the subscription to the main source until another Publisher
 * signals a value or completes.
 *
 * @param <T> the main source value type
 * @param <U> the other source type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDelaySubscription<T, U> extends FluxSource<T, T> {

	final Publisher<U> other;

	public FluxDelaySubscription(Publisher<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		other.subscribe(new DelaySubscriptionOtherSubscriber<>(s, source));
	}

	static final class DelaySubscriptionOtherSubscriber<T, U>
			extends Operators.DeferredSubscriptionSubscriber<U, T> {

		final Publisher<? extends T> source;

		Subscription s;

		boolean done;

		public DelaySubscriptionOtherSubscriber(Subscriber<? super T> actual, Publisher<? extends T> source) {
			super(actual);
			this.source = source;
		}

		@Override
		public void cancel() {
			s.cancel();
			super.cancel();
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
		public void onNext(U t) {
			if (done) {
				return;
			}
			done = true;
			s.cancel();

			subscribeSource();
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

			subscribeSource();
		}

		void subscribeSource() {
			source.subscribe(new DelaySubscriptionMainSubscriber<>(subscriber, this));
		}

		static final class DelaySubscriptionMainSubscriber<T> implements Subscriber<T> {

			final Subscriber<? super T> actual;

			final Operators.DeferredSubscriptionSubscriber<?, ?> arbiter;

			public DelaySubscriptionMainSubscriber(Subscriber<? super T> actual,
															Operators.DeferredSubscriptionSubscriber<?, ?> arbiter) {
				this.actual = actual;
				this.arbiter = arbiter;
			}

			@Override
			public void onSubscribe(Subscription s) {
				arbiter.set(s);
			}

			@Override
			public void onNext(T t) {
				actual.onNext(t);
			}

			@Override
			public void onError(Throwable t) {
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				actual.onComplete();
			}


		}
	}
}

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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



/**
 * Delays the subscription to the main source until another Publisher
 * signals a value or completes.
 *
 * @param <T> the main source value type
 * @param <U> the other source type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDelaySubscription<T, U> extends FluxSource<T, T> {

	final Publisher<U> other;

	FluxDelaySubscription(Flux<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		other.subscribe(new DelaySubscriptionOtherSubscriber<>(s, source));
	}

	static final class DelaySubscriptionOtherSubscriber<T, U>
			extends Operators.DeferredSubscription implements InnerOperator<U, T> {

		final Publisher<? extends T> source;

		final Subscriber<? super T> actual;

		Subscription s;

		boolean done;

		DelaySubscriptionOtherSubscriber(Subscriber<? super T> actual, Publisher<? extends T> source) {
			this.actual = actual;
			this.source = source;

		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return actual;
			if (key == BooleanAttr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
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

				actual.onSubscribe(this);

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
				Operators.onErrorDropped(t);
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

			subscribeSource();
		}

		void subscribeSource() {
			source.subscribe(new DelaySubscriptionMainSubscriber<>(actual, this));
		}
	}

	static final class DelaySubscriptionMainSubscriber<T>
			implements InnerConsumer<T> {

		final Subscriber<? super T> actual;

		final DelaySubscriptionOtherSubscriber<?, ?> arbiter;

		DelaySubscriptionMainSubscriber(Subscriber<? super T> actual,
				DelaySubscriptionOtherSubscriber<?, ?> arbiter) {
			this.actual = actual;
			this.arbiter = arbiter;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.ACTUAL) return actual;

			return null;
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

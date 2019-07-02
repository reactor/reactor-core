/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Objects;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Delays the subscription to the main source until another Publisher
 * signals a value or completes.
 *
 * @param <T> the main source value type
 * @param <U> the other source type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDelaySubscription<T, U> extends InternalFluxOperator<T, T>
		implements Consumer<FluxDelaySubscription.DelaySubscriptionOtherSubscriber<T, U>> {

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
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		other.subscribe(new DelaySubscriptionOtherSubscriber<>(actual, this));
		return null;
	}

	@Override
	public void accept(DelaySubscriptionOtherSubscriber<T, U> s) {
		source.subscribe(new DelaySubscriptionMainSubscriber<>(s.actual, s));
	}

	static final class DelaySubscriptionOtherSubscriber<T, U>
			extends Operators.DeferredSubscription implements InnerOperator<U, T> {

		final Consumer<FluxDelaySubscription.DelaySubscriptionOtherSubscriber<T, U>> source;

		final CoreSubscriber<? super T> actual;

		Subscription s;

		boolean done;

		DelaySubscriptionOtherSubscriber(CoreSubscriber<? super T> actual,
				Consumer<FluxDelaySubscription.DelaySubscriptionOtherSubscriber<T, U>> source) {
			this.actual = actual;
			this.source = source;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return actual;
			if (key == Attr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
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

			source.accept(this);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
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

			source.accept(this);
		}
	}

	static final class DelaySubscriptionMainSubscriber<T>
			implements InnerConsumer<T> {

		final CoreSubscriber<? super T> actual;

		final DelaySubscriptionOtherSubscriber<?, ?> arbiter;

		DelaySubscriptionMainSubscriber(CoreSubscriber<? super T> actual,
				DelaySubscriptionOtherSubscriber<?, ?> arbiter) {
			this.actual = actual;
			this.arbiter = arbiter;
		}

		@Override
		public Context currentContext() {
			return arbiter.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return actual;

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

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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Intercepts the onSubscribe call and makes sure calls to Subscription methods only
 * happen after the child Subscriber has returned from its onSubscribe method.
 * <p>
 * <p>This helps with child Subscribers that don't expect a recursive call from
 * onSubscribe into their onNext because, for example, they request immediately from their
 * onSubscribe but don't finish their preparation before that and onNext runs into a
 * half-prepared state. This can happen with non Rx mentality based Subscribers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 3.0
 */
final class FluxAwaitOnSubscribe<T> extends FluxOperator<T, T> {

	FluxAwaitOnSubscribe(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new PostOnSubscribeSubscriber<>(s), ctx);
	}

	static final class PostOnSubscribeSubscriber<T> implements InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PostOnSubscribeSubscriber, Subscription>
				S =
				AtomicReferenceFieldUpdater.newUpdater(PostOnSubscribeSubscriber.class,
						Subscription.class,
						"s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PostOnSubscribeSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PostOnSubscribeSubscriber.class,
						"requested");

		PostOnSubscribeSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {

				actual.onSubscribe(this);

				if (Operators.setOnce(S, this, s)) {
					long r = REQUESTED.getAndSet(this, 0L);
					if (r != 0L) {
						s.request(r);
					}
				}
			}
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

		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			}
			else {
				if (Operators.validate(n)) {
					Operators.getAndAddCap(REQUESTED, this, n);
					a = s;
					if (a != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							a.request(n);
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}
	}
}

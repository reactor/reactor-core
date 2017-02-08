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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;

/**
 * Hides the identities of the upstream Publisher object and its Subscription
 * as well. 
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxHide<T> extends FluxSource<T, T> {

	FluxHide(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new HideSubscriber<>(s));
	}

	static final class HideSubscriber<T> implements Subscriber<T>, Subscription, Receiver {
		final Subscriber<? super T> actual;

		Subscription s;
		
		HideSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public Object upstream() {
			return s;
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
		public void onSubscribe(Subscription s) {
			this.s = s;
			actual.onSubscribe(this);
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

	static final class SuppressFuseableSubscriber<T>
			implements Producer, Receiver, Subscriber<T>, Fuseable.QueueSubscription<T> {

		final Subscriber<? super T> actual;

		Subscription s;

		public SuppressFuseableSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;

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
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}

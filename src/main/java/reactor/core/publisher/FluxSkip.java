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
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Skips the first N elements from a reactive stream.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSkip<T> extends FluxSource<T, T> {

	final Publisher<? extends T> source;

	final long n;

	FluxSkip(Publisher<? extends T> source, long n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.n = n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new SkipSubscriber<>(s, n));
	}

	//Fixme Does not implement ConditionalSubscriber until the full chain of operators
	// supports fully conditional, requesting N onSubscribe cost is offset

	static final class SkipSubscriber<T>
			implements Subscriber<T>, Receiver,
			           Producer, Subscription, Trackable {

		final Subscriber<? super T> actual;

		long remaining;

		Subscription s;

		SkipSubscriber(Subscriber<? super T> actual, long n) {
			this.actual = actual;
			this.remaining = n;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				long n = remaining;
				actual.onSubscribe(this);
				s.request(n);
			}
		}

		@Override
		public void onNext(T t) {
			long r = remaining;
			if (r == 0L) {
				actual.onNext(t);
			}
			else {
				remaining = r - 1;
			}
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
		public boolean isStarted() {
			return s != null;
		}

		@Override
		public Object downstream() {
			return actual;
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
	}
}

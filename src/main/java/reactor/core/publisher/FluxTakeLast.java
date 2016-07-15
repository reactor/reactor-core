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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.publisher.FluxTakeLastOne.TakeLastOneSubscriber;
import reactor.core.Trackable;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTakeLast<T> extends FluxSource<T, T> implements Fuseable {

	final int n;

	public FluxTakeLast(Publisher<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (n == 0) {
			source.subscribe(new TakeLastZeroSubscriber<>(s));
		} else if (n == 1) {
			source.subscribe(new TakeLastOneSubscriber<>(s));
		} else {
			source.subscribe(new TakeLastManySubscriber<>(s, n));
		}
	}


	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	static final class TakeLastZeroSubscriber<T> implements Subscriber<T>, Producer, Subscription,
																	 Receiver {

		final Subscriber<? super T> actual;
		
		Subscription s;

		public TakeLastZeroSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
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
		public void onNext(T t) {
			// ignoring all values
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
		public Object downstream() {
			return actual;
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
		public Object upstream() {
			return s;
		}
	}

	static final class TakeLastManySubscriber<T>
			implements Subscriber<T>, Subscription, BooleanSupplier, Producer, Trackable, Receiver {

		final Subscriber<? super T> actual;

		final int n;

		volatile boolean cancelled;

		Subscription s;

		final ArrayDeque<T> buffer;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TakeLastManySubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(TakeLastManySubscriber.class, "requested");

		public TakeLastManySubscriber(Subscriber<? super T> actual, int n) {
			this.actual = actual;
			this.n = n;
			this.buffer = new ArrayDeque<>();
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				DrainUtils.postCompleteRequest(n, actual, buffer, REQUESTED, this, this);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			s.cancel();
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
		public void onNext(T t) {
			ArrayDeque<T> bs = buffer;

			if (bs.size() == n) {
				bs.poll();
			}
			bs.offer(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {

			DrainUtils.postComplete(actual, buffer, REQUESTED, this, this);
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public long getPending() {
			return buffer.size();
		}

		@Override
		public long getCapacity() {
			return n;
		}

		@Override
		public Object downstream() {
			return actual;
		}
	}
}

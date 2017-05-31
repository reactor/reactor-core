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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTakeLast<T> extends FluxSource<T, T> {

	final int n;

	FluxTakeLast(Flux<? extends T> source, int n) {
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
		}
		else {
			source.subscribe(new TakeLastManySubscriber<>(s, n));
		}
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	static final class TakeLastZeroSubscriber<T> implements InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		Subscription s;

		TakeLastZeroSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
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
		public Subscriber<? super T> actual() {
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
	}

	static final class TakeLastManySubscriber<T> extends ArrayDeque<T>
			implements BooleanSupplier, InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		final int n;

		volatile boolean cancelled;

		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TakeLastManySubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(TakeLastManySubscriber.class,
						"requested");

		TakeLastManySubscriber(Subscriber<? super T> actual, int n) {
			this.actual = actual;
			this.n = n;
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				DrainUtils.postCompleteRequest(n, actual, this, REQUESTED, this, this);
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
			if (size() == n) {
				poll();
			}
			offer(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == ScannableAttr.PARENT) return s;
			if (key == IntAttr.BUFFERED) return size();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}
	}
}

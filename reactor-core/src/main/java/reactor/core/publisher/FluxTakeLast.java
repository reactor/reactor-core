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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTakeLast<T> extends InternalFluxOperator<T, T> {

	final int n;

	FluxTakeLast(Flux<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (n == 0) {
			return new TakeLastZeroSubscriber<>(actual);
		}
		else {
			return new TakeLastManySubscriber<>(actual, n);
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	static final class TakeLastZeroSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		Subscription s;

		TakeLastZeroSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
		public CoreSubscriber<? super T> actual() {
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

		final CoreSubscriber<? super T> actual;

		final int n;

		volatile boolean cancelled;

		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TakeLastManySubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(TakeLastManySubscriber.class,
						"requested");

		TakeLastManySubscriber(CoreSubscriber<? super T> actual, int n) {
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.PARENT) return s;
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}
	}
}

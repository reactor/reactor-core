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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Skips the last N elements from the source stream.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSkipLast<T> extends InternalFluxOperator<T, T> {

	final int n;

	FluxSkipLast(Flux<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new SkipLastSubscriber<>(actual, n);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	//Fixme Does not implement ConditionalSubscriber until the full chain of operators
	// supports fully conditional, requesting N onSubscribe cost is offset

	static final class SkipLastSubscriber<T>
			extends ArrayDeque<T>
			implements InnerOperator<T, T> {
		final CoreSubscriber<? super T> actual;

		final int n;

		Subscription s;

		SkipLastSubscriber(CoreSubscriber<? super T> actual, int n) {
			this.actual = actual;
			this.n = n;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(n);
			}
		}

		@Override
		public void onNext(T t) {
			if (size() == n) {
				actual.onNext(pollFirst());
			}
			offerLast(t);

		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
			Operators.onDiscardQueueWithClear(this, actual.currentContext(), null);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			Operators.onDiscardQueueWithClear(this, actual.currentContext(), null);
		}


		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return n;
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
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
			Operators.onDiscardQueueWithClear(this, actual.currentContext(), null);
		}
	}
}

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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Skips the last N elements from the source stream.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSkipLast<T> extends FluxOperator<T, T> {

	final int n;

	FluxSkipLast(Flux<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new SkipLastSubscriber<>(s, n), ctx);
	}

	//Fixme Does not implement ConditionalSubscriber until the full chain of operators
	// supports fully conditional, requesting N onSubscribe cost is offset

	static final class SkipLastSubscriber<T>
			extends ArrayDeque<T>
			implements InnerOperator<T, T> {
		final Subscriber<? super T> actual;

		final int n;

		Subscription s;

		SkipLastSubscriber(Subscriber<? super T> actual, int n) {
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
				actual.onNext(poll());
			}
			offer(t);

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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == IntAttr.PREFETCH) return n;
			if (key == IntAttr.BUFFERED) return size();

			return InnerOperator.super.scanUnsafe(key);
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
}

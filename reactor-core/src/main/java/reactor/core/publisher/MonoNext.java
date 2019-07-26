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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Emits a single item at most from the source.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoNext<T> extends MonoFromFluxOperator<T, T> {

	MonoNext(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new NextSubscriber<>(actual);
	}

	static final class NextSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		Subscription s;

		boolean done;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<NextSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(NextSubscriber.class, "wip");

		NextSubscriber(CoreSubscriber<? super T> actual) {
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
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			s.cancel();
			actual.onNext(t);
			onComplete();
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
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (WIP.compareAndSet(this, 0, 1)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}
}

/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Emits a single boolean true if any of the values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with true if
 * the predicate matches a value.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoAny<T> extends MonoFromFluxOperator<T, Boolean>
		implements Fuseable {

	final Predicate<? super T> predicate;

	MonoAny(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Boolean> actual) {
		return new AnySubscriber<T>(actual, predicate);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class AnySubscriber<T> extends Operators.MonoSubscriber<T, Boolean>  {
		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		AnySubscriber(CoreSubscriber<? super Boolean> actual, Predicate<? super T> predicate) {
			super(actual);
			this.predicate = predicate;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
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
		public void onNext(T t) {

			if (done) {
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				done = true;
				actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}
			if (b) {
				done = true;
				s.cancel();

				complete(true);
			}
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
			complete(false);
		}

	}
}

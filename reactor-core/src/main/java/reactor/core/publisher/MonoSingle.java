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

import java.util.NoSuchElementException;
import java.util.Objects;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Expects and emits a single item from the source or signals
 * NoSuchElementException(or a default generated value) for empty source,
 * IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSingle<T> extends MonoFromFluxOperator<T, T>
		implements Fuseable {

	final T       defaultValue;
	final boolean completeOnEmpty;

	MonoSingle(Flux<? extends T> source) {
		super(source);
		this.defaultValue = null;
		this.completeOnEmpty = false;
	}

	MonoSingle(Flux<? extends T> source,
			@Nullable T defaultValue,
			boolean completeOnEmpty) {
		super(source);
		this.defaultValue = completeOnEmpty ? defaultValue :
				Objects.requireNonNull(defaultValue, "defaultValue");
		this.completeOnEmpty = completeOnEmpty;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new SingleSubscriber<>(actual, defaultValue, completeOnEmpty);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SingleSubscriber<T> extends Operators.MonoSubscriber<T, T>  {

		@Nullable
		final T       defaultValue;
		final boolean completeOnEmpty;

		Subscription s;

		int count;

		boolean done;

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		SingleSubscriber(CoreSubscriber<? super T> actual,
				@Nullable T defaultValue,
				boolean completeOnEmpty) {
			super(actual);
			this.defaultValue = defaultValue;
			this.completeOnEmpty = completeOnEmpty;
		}

		@Override
		public void request(long n) {
			super.request(n);
			if (n > 0L) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
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
			if (isCancelled()) {
				//this helps differentiating a duplicate malformed signal "done" from a count > 1 "done"
				discard(t);
				return;
			}
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (++count > 1) {
				discard(t);
				//mark as both cancelled and done
				cancel();
				onError(new IndexOutOfBoundsException("Source emitted more than one item"));
			}
			else {
				setValue(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			T v = this.value;
			if (v != null) {
				discard(v);
				this.value = null;
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			int c = count;
			if (c == 0) {

				if (completeOnEmpty) {
					actual.onComplete();
					return;
				}

				T t = defaultValue;

				if (t != null) {
					complete(t);
				}
				else {
					actual.onError(Operators.onOperatorError(this,
							new NoSuchElementException("Source was empty"),
							actual.currentContext()));
				}
			}
			else if (c == 1) {
				complete(this.value);
			}
		}

	}
}

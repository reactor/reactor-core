/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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

	static final class AnySubscriber<T> implements InnerOperator<T, Boolean>,
	                                               Fuseable, //for constants only
	                                               QueueSubscription<Boolean> {

		final CoreSubscriber<? super Boolean> actual;
		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		boolean hasRequest;

		volatile     int                                              state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AnySubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(AnySubscriber.class, "state");

		AnySubscriber(CoreSubscriber<? super Boolean> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PREFETCH) return 0;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super Boolean> actual() {
			return this.actual;
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void request(long n) {
			if (!hasRequest) {
				hasRequest = true;

				final int state = this.state;
				if ((state & 1) == 1) {
					return;
				}

				if (STATE.compareAndSet(this, state, state | 1)) {
					if (state == 0) {
						s.request(Long.MAX_VALUE);
					}
					else {
						// completed before request means source was empty
						actual.onNext(false);
						actual.onComplete();
					}
				}
			}
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
				Operators.onDiscard(t, this.actual.currentContext());
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

				this.actual.onNext(true);
				this.actual.onComplete();
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

			if (hasRequest) {
				this.actual.onNext(false);
				this.actual.onComplete();
				return;
			}

			final int state = this.state;
			if (state == 0 && STATE.compareAndSet(this, 0, 2)) {
				return;
			}

			actual.onNext(false);
			actual.onComplete();
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public Boolean poll() {
			return null;
		}

		@Override
		public void clear() {

		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public int size() {
			return 0;
		}
	}
}

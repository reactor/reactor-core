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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link java.util.function.Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoCallable<T> extends Mono<T>
		implements Callable<T>, Fuseable, SourceProducer<T> {

	final Callable<? extends T> callable;

	MonoCallable(Callable<? extends T> callable) {
		this.callable = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		actual.onSubscribe(new MonoCallableSubscription<>(actual, this.callable));
	}

	@Override
	@Nullable
	public T block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	@Nullable
	public T block(Duration m) {
		try {
			return callable.call();
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}
	}

	@Override
	@Nullable
	public T call() throws Exception {
		return callable.call();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static class MonoCallableSubscription<T>
			implements InnerProducer<T>, Fuseable, QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;
		final Callable<? extends T>     callable;

		boolean done;

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoCallableSubscription> STATE =
				AtomicIntegerFieldUpdater.newUpdater(MonoCallableSubscription.class,
						"state");

		static final int CANCELLED_FLAG      = 0b1000_0000_0000_0000_0000_0000_0000_0000;
		static final int TERMINATED_FLAG     = 0b0100_0000_0000_0000_0000_0000_0000_0000;
		static final int REQUESTED_ONCE_FLAG = 0b0010_0000_0000_0000_0000_0000_0000_0000;

		MonoCallableSubscription(CoreSubscriber<? super T> actual, Callable<? extends T> callable) {
			this.actual = actual;
			this.callable = callable;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public T poll() {
			if (this.done) {
				return null;
			}

			this.done = true;

			try {
				return this.callable.call();
			}
			catch (Throwable e) {
				throw Exceptions.propagate(e);
			}
		}

		@Override
		public void request(long n) {
			int previousState = makeRequestedOnce(this);

			if (isCancelled(previousState) || isRequestedOnce(previousState)) {
				return;
			}

			final CoreSubscriber<? super T> s = this.actual;

			final T value;
			try {
				value = this.callable.call();
			}
			catch (Exception e) {
				previousState = makeTerminated(this);

				if (isCancelled(previousState)) {
					Operators.onErrorDropped(e, s.currentContext());
					return;
				}

				s.onError(e);
				return;
			}

			previousState = makeTerminated(this);
			if (isCancelled(previousState)) {
				Operators.onDiscard(value, s.currentContext());
				return;
			}

			if (value != null) {
				s.onNext(value);
			}

			s.onComplete();
		}

		@Override
		public void cancel() {
			makeCancelled(this);
		}

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode & SYNC;
		}

		@Override
		public int size() {
			return this.done ? 0 : 1;
		}

		@Override
		public boolean isEmpty() {
			return this.done;
		}

		@Override
		public void clear() {
			this.done = true;
		}

		static <T> int makeRequestedOnce(MonoCallableSubscription<T> instance) {
			for (;;) {
				final int state = instance.state;

				if (isCancelled(state) || isRequestedOnce(state)) {
					return state;
				}

				final int nextState = state | REQUESTED_ONCE_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static <T> int makeTerminated(MonoCallableSubscription<T> instance) {
			for (;;) {
				final int state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				final int nextState = state | TERMINATED_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static <T> int makeCancelled(MonoCallableSubscription<T> instance) {
			for (;;) {
				final int state = instance.state;

				if (isCancelled(state) || isTerminated(state)) {
					return state;
				}

				final int nextState = state | CANCELLED_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean isCancelled(int state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static boolean isRequestedOnce(int state) {
			return (state & REQUESTED_ONCE_FLAG) == REQUESTED_ONCE_FLAG;
		}

		static boolean isTerminated(int state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}
	}
}

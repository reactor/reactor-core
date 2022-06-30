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

		volatile int requestedOnce;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoCallableSubscription> REQUESTED_ONCE =
				AtomicIntegerFieldUpdater.newUpdater(MonoCallableSubscription.class,
						"requestedOnce");

		volatile boolean cancelled;

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
			if (this.cancelled) {
				return;
			}

			if (this.requestedOnce == 1 || !REQUESTED_ONCE.compareAndSet(this, 0 , 1)) {
				return;
			}

			final CoreSubscriber<? super T> s = this.actual;

			final T value;
			try {
				value = this.callable.call();
			}
			catch (Exception e) {
				if (this.cancelled) {
					Operators.onErrorDropped(e, s.currentContext());
					return;
				}

				s.onError(e);
				return;
			}


			if (this.cancelled) {
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
			this.cancelled = true;
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
	}
}

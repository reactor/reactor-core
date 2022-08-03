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
import java.util.function.Supplier;


import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Executes a Supplier function and emits a single value to each individual Subscriber.
 *
 * @param <T> the returned value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSupplier<T> 
extends Mono<T>
		implements Callable<T>, Fuseable, SourceProducer<T>  {

	final Supplier<? extends T> supplier;

	MonoSupplier(Supplier<? extends T> callable) {
		this.supplier = Objects.requireNonNull(callable, "callable");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		actual.onSubscribe(new MonoSupplierSubscription<>(actual, supplier));
	}
	
	@Override
	@Nullable
	public T block(Duration m) {
		return supplier.get();
	}

	@Override
	@Nullable
	public T block() {
		//the duration is ignored above
		return block(Duration.ZERO);
	}
	
	@Override
	@Nullable
	public T call() throws Exception {
		return supplier.get();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static class MonoSupplierSubscription<T>
			implements InnerProducer<T>, Fuseable, QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;
		final Supplier<? extends T>     supplier;

		boolean done;

		volatile int requestedOnce;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoSupplierSubscription> REQUESTED_ONCE =
				AtomicIntegerFieldUpdater.newUpdater(MonoSupplierSubscription.class, "requestedOnce");

		volatile boolean cancelled;

		MonoSupplierSubscription(CoreSubscriber<? super T> actual, Supplier<? extends T> callable) {
			this.actual = actual;
			this.supplier = callable;
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

			return this.supplier.get();
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
				value = this.supplier.get();
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

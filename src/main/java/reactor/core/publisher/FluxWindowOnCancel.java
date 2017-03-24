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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;

/**
 * Splits the source sequence into possibly overlapping publishers.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowOnCancel<T> extends FluxSource<T, Flux<T>> {

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	FluxWindowOnCancel(Flux<? extends T> source,
			Supplier<? extends Queue<T>> processorQueueSupplier) {
		super(source);
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super Flux<T>> s) {
		source.subscribe(new WindowOnCancelSubscriber<>(s, processorQueueSupplier));
	}

	static final class WindowOnCancelSubscriber<T>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final Subscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOnCancelSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowOnCancelSubscriber.class, "wip");

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOnCancelSubscriber> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(WindowOnCancelSubscriber.class, "once");

		Subscription s;

		UnicastProcessor<T> window;

		boolean done;

		WindowOnCancelSubscriber(Subscriber<? super Flux<T>> actual,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.processorQueueSupplier = processorQueueSupplier;
			this.wip = 1;
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
				Operators.onNextDropped(t);
				return;
			}
			UnicastProcessor<T> w = window;
			if (w == null || w.cancelled) {
				WIP.getAndIncrement(this);

				w = new UnicastProcessor<>(processorQueueSupplier.get(), this);
				window = w;

				actual.onNext(w);
			}

			w.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				s.request(n);
			}
		}

		@Override
		public void cancel() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WIP.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public boolean isDisposed() {
			return once == 1 || done;
		}

		@Override
		public Subscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return s;
				case CANCELLED:
					return once == 1;
				case TERMINATED:
					return done;
			}
			return InnerOperator.super.scan(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}

	}

}

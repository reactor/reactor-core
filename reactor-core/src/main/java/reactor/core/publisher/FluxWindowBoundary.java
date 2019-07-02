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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Splits the source sequence into continuous, non-overlapping windowEnds
 * where the window boundary is signalled by another Publisher
 *
 * @param <T> the input value type
 * @param <U> the boundary publisher's type (irrelevant)
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowBoundary<T, U> extends InternalFluxOperator<T, Flux<T>> {

	final Publisher<U> other;

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	FluxWindowBoundary(Flux<? extends T> source, Publisher<U> other,
			Supplier<? extends Queue<T>> processorQueueSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Flux<T>> actual) {
		WindowBoundaryMain<T, U> main = new WindowBoundaryMain<>(actual,
				processorQueueSupplier, processorQueueSupplier.get());

		actual.onSubscribe(main);

		if (main.emit(main.window)) {
			other.subscribe(main.boundary);

			return main;
		}
		else {
			return null;
		}
	}

	static final class WindowBoundaryMain<T, U>
			implements InnerOperator<T, Flux<T>>, Disposable {

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final WindowBoundaryOther<U> boundary;

		final Queue<Object>                   queue;
		final CoreSubscriber<? super Flux<T>> actual;

		UnicastProcessor<T> window;

		volatile Subscription s;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowBoundaryMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(WindowBoundaryMain.class, Subscription.class, "s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowBoundaryMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowBoundaryMain.class, "requested");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowBoundaryMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(WindowBoundaryMain.class, Throwable.class, "error");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "windowCount");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "wip");

		boolean done;

		static final Object BOUNDARY_MARKER = new Object();

		static final Object DONE = new Object();

		WindowBoundaryMain(CoreSubscriber<? super Flux<T>> actual,
				Supplier<? extends Queue<T>> processorQueueSupplier,
				Queue<T> processorQueue) {
			this.actual = actual;
			this.processorQueueSupplier = processorQueueSupplier;
			this.window = new UnicastProcessor<>(processorQueue, this);
			WINDOW_COUNT.lazySet(this, 2);
			this.boundary = new WindowBoundaryOther<>(this);
			this.queue = Queues.unboundedMultiproducer().get();
		}

		@Override
		public final CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ERROR) return error;
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.BUFFERED) return queue.size();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(boundary, window);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			synchronized (this) {
				queue.offer(t);
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			boundary.cancel();
			if (Exceptions.addThrowable(ERROR, this, t)) {
				drain();
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			boundary.cancel();
			synchronized (this) {
				queue.offer(DONE);
			}
			drain();
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				cancelMain();
				boundary.cancel();
			}
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		void cancelMain() {
			Operators.terminate(S, this);
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		void boundaryNext() {
			synchronized (this) {
				queue.offer(BOUNDARY_MARKER);
			}

			if (cancelled != 0) {
				boundary.cancel();
			}

			drain();
		}

		void boundaryError(Throwable e) {
			cancelMain();
			if (Exceptions.addThrowable(ERROR, this, e)) {
				drain();
			} else {
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		void boundaryComplete() {
			cancelMain();
			synchronized (this) {
				queue.offer(DONE);
			}
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super Flux<T>> a = actual;
			final Queue<Object> q = queue;
			UnicastProcessor<T> w = window;

			int missed = 1;

			for (;;) {

				for (;;) {
					if (error != null) {
						q.clear();
						Throwable e = Exceptions.terminate(ERROR, this);
						if (e != Exceptions.TERMINATED) {
							w.onError(e);

							a.onError(e);
						}
						return;
					}

					Object o = q.poll();

					if (o == null) {
						break;
					}

					if (o == DONE) {
						q.clear();

						w.onComplete();

						a.onComplete();
						return;
					}
					if (o != BOUNDARY_MARKER) {

						@SuppressWarnings("unchecked")
						T v = (T)o;
						w.onNext(v);
					}
					if (o == BOUNDARY_MARKER) {
						w.onComplete();

						if (cancelled == 0) {
							if (requested != 0L) {
								Queue<T> pq = processorQueueSupplier.get();

								WINDOW_COUNT.getAndIncrement(this);

								w = new UnicastProcessor<>(pq, this);
								window = w;

								a.onNext(w);

								if (requested != Long.MAX_VALUE) {
									REQUESTED.decrementAndGet(this);
								}
							} else {
								q.clear();
								cancelMain();
								boundary.cancel();

								a.onError(Exceptions.failWithOverflow("Could not create new window due to lack of requests"));
								return;
							}
						}
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean emit(UnicastProcessor<T> w) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(w);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			} else {
				cancel();

				actual.onError(Exceptions.failWithOverflow("Could not emit buffer due to lack of requests"));

				return false;
			}
		}
	}

	static final class WindowBoundaryOther<U>
			extends Operators.DeferredSubscription
			implements InnerConsumer<U> {

		final WindowBoundaryMain<?, U> main;

		WindowBoundaryOther(WindowBoundaryMain<?, U> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return main;
			}
			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(U t) {
			main.boundaryNext();
		}

		@Override
		public void onError(Throwable t) {
			main.boundaryError(t);
		}

		@Override
		public void onComplete() {
			main.boundaryComplete();
		}
	}
}

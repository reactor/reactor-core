/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;

/**
 * Splits the source sequence into continuous, non-overlapping windowEnds 
 * where the window boundary is signalled by another Publisher
 * 
 * @param <T> the input value type
 * @param <U> the boundary publisher's type (irrelevant)
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowBoundary<T, U> extends FluxSource<T, Flux<T>> {

	final Publisher<U> other;

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	final Supplier<? extends Queue<Object>> drainQueueSupplier;

	public FluxWindowBoundary(Publisher<? extends T> source, Publisher<U> other,
			Supplier<? extends Queue<T>> processorQueueSupplier,
			Supplier<? extends Queue<Object>> drainQueueSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.drainQueueSupplier = Objects.requireNonNull(drainQueueSupplier, "drainQueueSupplier");
	}

	@Override
	public long getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super Flux<T>> s) {
		WindowBoundaryMain<T, U> main = new WindowBoundaryMain<>(s,
				processorQueueSupplier, processorQueueSupplier.get(), drainQueueSupplier.get());

		s.onSubscribe(main);

		if (main.emit(main.window)) {
			other.subscribe(main.boundary);

			source.subscribe(main);
		}
	}

	static final class WindowBoundaryMain<T, U>
			implements Subscriber<T>, Subscription, Disposable {

		final Subscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final WindowBoundaryOther<U> boundary;

		final Queue<Object> queue;

		UnicastProcessor<T> window;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowBoundaryMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(WindowBoundaryMain.class, Subscription.class, "s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowBoundaryMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowBoundaryMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WindowBoundaryMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(WindowBoundaryMain.class, Throwable.class, "error");

		volatile int open;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> OPEN =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "open");

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowBoundaryMain> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(WindowBoundaryMain.class, "once");

		static final Object BOUNDARY_MARKER = new Object();
		
		static final Object DONE = new Object();

		public WindowBoundaryMain(Subscriber<? super Flux<T>> actual,
				Supplier<? extends Queue<T>> processorQueueSupplier,
				Queue<T> processorQueue, Queue<Object> queue) {
			this.actual = actual;
			this.processorQueueSupplier = processorQueueSupplier;
			this.window = new UnicastProcessor<>(processorQueue, this);
			this.open = 2;
			this.boundary = new WindowBoundaryOther<>(this);
			this.queue = queue;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (this) {
				queue.offer(t);
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			boundary.cancel();
			if (Exceptions.addThrowable(ERROR, this, t)) {
				drain();
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			boundary.cancel();
			synchronized (this) {
				queue.offer(DONE);
			}
			drain();
		}

		@Override
		public void dispose() {
			if (OPEN.decrementAndGet(this) == 0) {
				cancelMain();
				boundary.cancel();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		void cancelMain() {
			Operators.terminate(S, this);
		}

		@Override
		public void cancel() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		void boundaryNext() {
			synchronized (this) {
				queue.offer(BOUNDARY_MARKER);
			}

			if (once != 0) {
				boundary.cancel();
			}

			drain();
		}

		void boundaryError(Throwable e) {
			cancelMain();
			if (Exceptions.addThrowable(ERROR, this, e)) {
				drain();
			} else {
				Operators.onErrorDropped(e);
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
						
						if (once == 0) {
							if (requested != 0L) {
								Queue<T> pq = processorQueueSupplier.get();

								OPEN.getAndIncrement(this);

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
			implements Subscriber<U> {

		final WindowBoundaryMain<?, U> main;

		public WindowBoundaryOther(WindowBoundaryMain<?, U> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
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

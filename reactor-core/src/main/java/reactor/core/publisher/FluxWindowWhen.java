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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * Splits the source sequence into potentially overlapping windowEnds controlled by items
 * of a start Publisher and end Publishers derived from the start values.
 *
 * @param <T> the source value type
 * @param <U> the window starter value type
 * @param <V> the window end value type (irrelevant)
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowWhen<T, U, V> extends InternalFluxOperator<T, Flux<T>> {

	final Publisher<U> start;

	final Function<? super U, ? extends Publisher<V>> end;

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	FluxWindowWhen(Flux<? extends T> source,
			Publisher<U> start,
			Function<? super U, ? extends Publisher<V>> end,
			Supplier<? extends Queue<T>> processorQueueSupplier) {
		super(source);
		this.start = Objects.requireNonNull(start, "start");
		this.end = Objects.requireNonNull(end, "end");
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	@Nullable
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Flux<T>> actual) {
		WindowWhenMainSubscriber<T, U, V> main = new WindowWhenMainSubscriber<>(actual,
				start, end, processorQueueSupplier);
		actual.onSubscribe(main);

		if (main.cancelled) {
			return null;
		}
		WindowWhenOpenSubscriber<T, U> os = new WindowWhenOpenSubscriber<>(main);

		if (WindowWhenMainSubscriber.BOUNDARY.compareAndSet(main,null, os)) {
			WindowWhenMainSubscriber.OPEN_WINDOW_COUNT.incrementAndGet(main);
			start.subscribe(os);
			return main;
		}
		else {
			return null;
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class WindowWhenMainSubscriber<T, U, V>
			extends QueueDrainSubscriber<T, Object, Flux<T>> {

		final Publisher<U> open;
		final Function<? super U, ? extends Publisher<V>> close;
		final Supplier<? extends Queue<T>> processorQueueSupplier;
		final Disposable.Composite resources;

		Subscription s;

		volatile Disposable boundary;
		static final AtomicReferenceFieldUpdater<WindowWhenMainSubscriber, Disposable> BOUNDARY =
				AtomicReferenceFieldUpdater.newUpdater(WindowWhenMainSubscriber.class, Disposable.class, "boundary");

		final List<Sinks.Many<T>> windows;

		volatile long openWindowCount;
		static final AtomicLongFieldUpdater<WindowWhenMainSubscriber> OPEN_WINDOW_COUNT =
				AtomicLongFieldUpdater.newUpdater(WindowWhenMainSubscriber.class, "openWindowCount");

		WindowWhenMainSubscriber(CoreSubscriber<? super Flux<T>> actual,
				Publisher<U> open, Function<? super U, ? extends Publisher<V>> close,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			super(actual, Queues.unboundedMultiproducer().get());
			this.open = open;
			this.close = close;
			this.processorQueueSupplier = processorQueueSupplier;
			this.resources = Disposables.composite();
			this.windows = new ArrayList<>();
			OPEN_WINDOW_COUNT.lazySet(this, 1);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (fastEnter()) {
				for (Sinks.Many<T> w : windows) {
					w.emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);
				}
				if (leave(-1) == 0) {
					return;
				}
			} else {
				queue.offer(t);
				if (!enter()) {
					return;
				}
			}
			drainLoop();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			error = t;
			done = true;

			if (enter()) {
				drainLoop();
			}

			if (OPEN_WINDOW_COUNT.decrementAndGet(this) == 0) {
				resources.dispose();
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			if (enter()) {
				drainLoop();
			}

			if (OPEN_WINDOW_COUNT.decrementAndGet(this) == 0) {
				resources.dispose();
			}
		}

		void error(Throwable t) {
			s.cancel();
			resources.dispose();
			OperatorDisposables.dispose(BOUNDARY, this);

			actual.onError(t);
		}

		@Override
		public void request(long n) {
			requested(n);
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		void dispose() {
			resources.dispose();
			OperatorDisposables.dispose(BOUNDARY, this);
		}

		void drainLoop() {
			final Queue<Object> q = queue;
			final Subscriber<? super Flux<T>> a = actual;
			final List<Sinks.Many<T>> ws = this.windows;
			int missed = 1;

			for (;;) {

				for (;;) {
					boolean d = done;
					Object o = q.poll();

					boolean empty = o == null;

					if (d && empty) {
						dispose();
						Throwable e = error;
						if (e != null) {
							actual.onError(e);
							for (Sinks.Many<T> w : ws) {
								w.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
							}
						} else {
							actual.onComplete();
							for (Sinks.Many<T> w : ws) {
								w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
							}
						}
						ws.clear();
						return;
					}

					if (empty) {
						break;
					}

					if (o instanceof WindowOperation) {
						@SuppressWarnings("unchecked")
						WindowOperation<T, U> wo = (WindowOperation<T, U>) o;

						Sinks.Many<T> w = wo.w;
						if (w != null) {
							if (ws.remove(wo.w)) {
								wo.w.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);

								if (OPEN_WINDOW_COUNT.decrementAndGet(this) == 0) {
									dispose();
									return;
								}
							}
							continue;
						}

						if (cancelled) {
							continue;
						}


						w = Sinks.unsafe().many().unicast().onBackpressureBuffer(processorQueueSupplier.get());

						long r = requested();
						if (r != 0L) {
							ws.add(w);
							a.onNext(w.asFlux());
							if (r != Long.MAX_VALUE) {
								produced(1);
							}
						} else {
							cancelled = true;
							a.onError(Exceptions.failWithOverflow("Could not deliver new window due to lack of requests"));
							continue;
						}

						Publisher<V> p;

						try {
							p = Objects.requireNonNull(close.apply(wo.open), "The publisher supplied is null");
						} catch (Throwable e) {
							cancelled = true;
							a.onError(e);
							continue;
						}

						WindowWhenCloseSubscriber<T, V> cl = new WindowWhenCloseSubscriber<T, V>(this, w);

						if (resources.add(cl)) {
							OPEN_WINDOW_COUNT.getAndIncrement(this);

							p.subscribe(cl);
						}

						continue;
					}

					for (Sinks.Many<T> w : ws) {
						@SuppressWarnings("unchecked")
						T t = (T) o;
						w.emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);
					}
				}

				missed = leave(-missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void open(U b) {
			queue.offer(new WindowOperation<T, U>(null, b));
			if (enter()) {
				drainLoop();
			}
		}

		void close(WindowWhenCloseSubscriber<T, V> w) {
			resources.remove(w);
			queue.offer(new WindowOperation<T, U>(w.w, null));
			if (enter()) {
				drainLoop();
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}
	}

	static final class WindowOperation<T, U> {
		final Sinks.Many<T> w;
		final U             open;
		WindowOperation(@Nullable Sinks.Many<T> w, @Nullable U open) {
			this.w = w;
			this.open = open;
		}
	}

	static final class WindowWhenOpenSubscriber<T, U>
			implements Disposable, Subscriber<U> {

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<WindowWhenOpenSubscriber, Subscription> SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(WindowWhenOpenSubscriber.class, Subscription.class, "subscription");

		final WindowWhenMainSubscriber<T, U, ?> parent;

		boolean done;

		WindowWhenOpenSubscriber(WindowWhenMainSubscriber<T, U, ?> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				subscription.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		public boolean isDisposed() {
			return subscription == Operators.cancelledSubscription();
		}

		@Override
		public void onNext(U t) {
			if (done) {
				return;
			}
			parent.open(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}
			done = true;
			parent.error(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			parent.onComplete();
		}
	}

	static final class WindowWhenCloseSubscriber<T, V>
			implements Disposable, Subscriber<V> {

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<WindowWhenCloseSubscriber, Subscription> SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(WindowWhenCloseSubscriber.class, Subscription.class, "subscription");

		final WindowWhenMainSubscriber<T, ?, V> parent;
		final Sinks.Many<T>                     w;

		boolean done;

		WindowWhenCloseSubscriber(WindowWhenMainSubscriber<T, ?, V> parent, Sinks.Many<T> w) {
			this.parent = parent;
			this.w = w;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				subscription.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		public boolean isDisposed() {
			return subscription == Operators.cancelledSubscription();
		}

		@Override
		public void onNext(V t) {
			if (done) {
				return;
			}
			done = true;
			dispose();
			parent.close(this);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}
			done = true;
			parent.error(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			parent.close(this);
		}
	}

}

/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFlattenIterable<T, R> extends InternalFluxOperator<T, R> implements Fuseable {

	final Function<? super T, ? extends Iterable<? extends R>> mapper;

	final int prefetch;

	final Supplier<Queue<T>> queueSupplier;

	FluxFlattenIterable(Flux<? extends T> source,
			Function<? super T, ? extends Iterable<? extends R>> mapper,
			int prefetch,
			Supplier<Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) throws Exception {

		if (source instanceof Callable) {
			T v = ((Callable<T>) source).call();

			if (v == null) {
				Operators.complete(actual);
				return null;
			}

			Spliterator<? extends R> sp;
			boolean knownToBeFinite;
			try {
				Iterable<? extends R> iter = mapper.apply(v);
				sp = iter.spliterator();
				knownToBeFinite = FluxIterable.checkFinite(sp);
			}
			catch (Throwable ex) {
				Context ctx = actual.currentContext();
				Throwable e_ = Operators.onNextError(v, ex, ctx);
				Operators.onDiscard(v, ctx);
				if (e_ != null) {
					Operators.error(actual, e_);
				}
				else {
					Operators.complete(actual);
				}
				return null;
			}

			// TODO return subscriber (tail-call optimization)?
			FluxIterable.subscribe(actual, sp, knownToBeFinite);
			return null;
		}
		return new FlattenIterableSubscriber<>(actual,
				mapper,
				prefetch,
				queueSupplier);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FlattenIterableSubscriber<T, R>
			implements InnerOperator<T, R>, QueueSubscription<R>, Consumer<R> {

		final CoreSubscriber<? super R> actual;

		final Function<? super T, ? extends Iterable<? extends R>> mapper;

		final int prefetch;

		final int limit;

		final Supplier<Queue<T>> queueSupplier;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FlattenIterableSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FlattenIterableSubscriber.class,
						"wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FlattenIterableSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FlattenIterableSubscriber.class,
						"requested");

		Subscription s;

		Queue<T> queue;

		volatile boolean done;

		volatile boolean cancelled;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlattenIterableSubscriber, Throwable>
				ERROR =
				AtomicReferenceFieldUpdater.newUpdater(FlattenIterableSubscriber.class,
						Throwable.class,
						"error");

		@Nullable
		Spliterator<? extends R> current;
		boolean currentKnownToBeFinite;

		boolean valueReady = false;

		R nextElement;

		int consumed;

		int fusionMode;

		FlattenIterableSubscriber(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Iterable<? extends R>> mapper,
				int prefetch,
				Supplier<Queue<T>> queueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = Operators.unboundedOrLimit(prefetch);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> qs =
							(QueueSubscription<T>) s;

					int m = qs.requestFusion(Fuseable.ANY);

					if (m == Fuseable.SYNC) {
						fusionMode = m;
						this.queue = qs;
						done = true;

						actual.onSubscribe(this);

						return;
					}
					else if (m == Fuseable.ASYNC) {
						fusionMode = m;
						this.queue = qs;

						actual.onSubscribe(this);

						s.request(Operators.unboundedOrPrefetch(prefetch));
						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if (fusionMode != Fuseable.ASYNC) {
				if (!queue.offer(t)) {
					Context ctx = actual.currentContext();
					onError(Operators.onOperatorError(s,Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
							ctx));
					Operators.onDiscard(t, ctx);
					return;
				}
			}
			drain(t);
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain(null);
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			done = true;
			drain(null);
		}

		@Override
		public void accept(R t) {
			valueReady = true;
			nextElement = t;
		}

		boolean hasNext(Spliterator<? extends R> spliterator) {
			if (!valueReady)
				spliterator.tryAdvance(this);
			return valueReady;
		}

		R next(Spliterator<? extends R> spliterator) {
			if (!valueReady && !hasNext(spliterator))
				throw new NoSuchElementException();
			else {
				valueReady = false;
				R t = nextElement;
				nextElement = null;
				return t;
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain(null);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				s.cancel();

				if (WIP.getAndIncrement(this) == 0) {
					Context context = actual.currentContext();
					Operators.onDiscardQueueWithClear(queue, context, null);
					Operators.onDiscard(nextElement, context);
					Operators.onDiscardMultiple(current, currentKnownToBeFinite, context);
				}
			}
		}

		//should be kept small and final to favor inlining
		final void resetCurrent() {
			current = null;
			currentKnownToBeFinite = false;
		}

		void drainAsync() {
			final Subscriber<? super R> a = actual;
			final Queue<T> q = queue;

			int missed = 1;
			Spliterator<? extends R> sp = current;
			boolean itFinite = currentKnownToBeFinite;

			for (; ; ) {

				if (sp == null) {

					if (cancelled) {
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						return;
					}

					Throwable ex = error;
					if (ex != null) {
						ex = Exceptions.terminate(ERROR, this);
						resetCurrent();
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						a.onError(ex);
						return;
					}

					boolean d = done;

					T t;

					try {
						t = q.poll();
					} catch (Throwable pollEx) {
						resetCurrent();
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						a.onError(pollEx);
						return;
					}

					boolean empty = t == null;

					if (d && empty) {
						a.onComplete();
						return;
					}

					if (!empty) {
						Iterable<? extends R> iterable;

						boolean isEmpty;

						try {
							iterable = mapper.apply(t);
							sp = iterable.spliterator();
							itFinite = FluxIterable.checkFinite(sp);

							isEmpty = itFinite ? sp.estimateSize() == 0 : !hasNext(sp);
						}
						catch (Throwable exc) {
							sp = null;
							itFinite = false; //reset explicitly
							Context ctx = actual.currentContext();
							Throwable e_ = Operators.onNextError(t, exc, ctx, s);
							Operators.onDiscard(t, ctx);
							if (e_ != null) {
								onError(e_);
							}
							continue;
						}

						if (isEmpty) {
							sp = null;
							itFinite = false; //reset explicitly
							int c = consumed + 1;
							if (c == limit) {
								consumed = 0;
								s.request(c);
							}
							else {
								consumed = c;
							}
							continue;
						}
					}
				}

				if (sp != null) {
					long r = requested;
					long e = 0L;

					while (e != r) {
						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(q, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						Throwable ex = error;
						if (ex != null) {
							ex = Exceptions.terminate(ERROR, this);
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(q, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							a.onError(ex);
							return;
						}

						R v;

						try {
							v = Objects.requireNonNull(next(sp),
									"iterator returned null");
						}
						catch (Throwable exc) {
							onError(Operators.onOperatorError(s, exc,
									actual.currentContext()));
							continue;
						}

						a.onNext(v);

						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(q, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						e++;

						boolean b;

						try {
							b = hasNext(sp);
						}
						catch (Throwable exc) {
							onError(Operators.onOperatorError(s, exc,
									actual.currentContext()));
							continue;
						}

						if (!b) {
							int c = consumed + 1;
							if (c == limit) {
								consumed = 0;
								s.request(c);
							}
							else {
								consumed = c;
							}
							sp = null;
							itFinite = false;
							resetCurrent();
							break;
						}
					}

					if (e == r) {
						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(q, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						Throwable ex = error;
						if (ex != null) {
							ex = Exceptions.terminate(ERROR, this);
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(q, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							a.onError(ex);
							return;
						}

						boolean d = done;
						boolean empty = q.isEmpty() && sp == null;

						if (d && empty) {
							resetCurrent();
							a.onComplete();
							return;
						}
					}

					if (e != 0L) {
						if (r != Long.MAX_VALUE) {
							REQUESTED.addAndGet(this, -e);
						}
					}

					if (sp == null) {
						continue;
					}
				}

				current = sp;
				currentKnownToBeFinite = itFinite;
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainSync() {
			final Subscriber<? super R> a = actual;

			int missed = 1;
			Spliterator<? extends R> sp = current;
			boolean itFinite = currentKnownToBeFinite;

			for (; ; ) {
				if (sp == null) {

					if (cancelled) {
						Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
						return;
					}

					boolean d = done;

					T t;
					Queue<T> q = queue;

					try {
						t = q.poll();
					} catch (Throwable pollEx) {
						resetCurrent();
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						a.onError(pollEx);
						return;
					}

					boolean empty = t == null;

					if (d && empty) {
						a.onComplete();
						return;
					}

					if (!empty) {
						Iterable<? extends R> iterable;

						boolean isEmpty;

						try {
							iterable = mapper.apply(t);
							sp = iterable.spliterator();
							itFinite = FluxIterable.checkFinite(sp);

							isEmpty = itFinite ? sp.estimateSize() == 0 : !hasNext(sp);
						}
						catch (Throwable exc) {
							resetCurrent();
							Context ctx = actual.currentContext();
							Throwable e_ = Operators.onNextError(t, exc, ctx, s);
							//note: if there is an exception, we can consider the iterator done,
							// so no attempt is made to discard remainder here
							Operators.onDiscard(t, ctx);
							if (e_ != null) {
								a.onError(e_);
								return;
							}
							continue;
						}

						if (isEmpty) {
							sp = null;
							itFinite = false;
							continue;
						}
					}
				}

				if (sp != null) {
					long r = requested;
					long e = 0L;

					while (e != r) {
						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(queue, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						R v;

						try {
							v = Objects.requireNonNull(next(sp), "iterator returned null");
						}
						catch (Throwable exc) {
							resetCurrent();
							a.onError(Operators.onOperatorError(s, exc, actual.currentContext()));
							return;
						}

						a.onNext(v);

						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(queue, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						e++;

						boolean b;

						try {
							b = hasNext(sp);
						}
						catch (Throwable exc) {
							resetCurrent();
							a.onError(Operators.onOperatorError(s, exc, actual.currentContext()));
							return;
						}

						if (!b) {
							sp = null;
							itFinite = false;
							resetCurrent();
							break;
						}
					}

					if (e == r) {
						if (cancelled) {
							resetCurrent();
							final Context context = actual.currentContext();
							Operators.onDiscardQueueWithClear(queue, context, null);
							Operators.onDiscard(nextElement, context);
							Operators.onDiscardMultiple(sp, itFinite, context);
							return;
						}

						boolean d = done;
						boolean empty = queue.isEmpty() && sp == null;

						if (d && empty) {
							resetCurrent();
							a.onComplete();
							return;
						}
					}

					if (e != 0L) {
						if (r != Long.MAX_VALUE) {
							REQUESTED.addAndGet(this, -e);
						}
					}

					if (sp == null) {
						continue;
					}
				}

				current = sp;
				currentKnownToBeFinite = itFinite;
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain(@Nullable T dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (dataSignal != null && cancelled) {
					Operators.onDiscard(dataSignal, actual.currentContext());
				}
				return;
			}

			if (fusionMode == SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		@Override
		public void clear() {
			final Context context = actual.currentContext();
			Operators.onDiscard(nextElement, context);
			Operators.onDiscardMultiple(current, currentKnownToBeFinite, context);
			resetCurrent();
			Operators.onDiscardQueueWithClear(queue, context, null);
		}

		@Override
		public boolean isEmpty() {
			Spliterator<? extends R> sp = current;
			if (sp != null) {
				return !hasNext(sp);
			}
			return queue.isEmpty(); // estimate
		}

		@Override
		@Nullable
		public R poll() {
			Spliterator<? extends R> sp = current;
			boolean itFinite;
			for (; ; ) {
				if (sp == null) {
					T v = queue.poll();
					if (v == null) {
						return null;
					}

					Iterable<? extends R> iterable;
					try {
						iterable = mapper.apply(v);
						sp = iterable.spliterator();
						itFinite = FluxIterable.checkFinite(sp);
					}
					catch (Throwable error) {
						Operators.onDiscard(v, actual.currentContext());
						throw error;
					}

					if (!hasNext(sp)) {
						continue;
					}
					current = sp;
					currentKnownToBeFinite = itFinite;
				}
				else if (!hasNext(sp)) {
					sp = null;
					continue;
				}

				R r = Objects.requireNonNull(next(sp), "iterator returned null");

				if (!hasNext(sp)) {
					resetCurrent();
				}

				return r;
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & SYNC) != 0 && fusionMode == SYNC) {
				return SYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size(); // estimate
		}
	}
}

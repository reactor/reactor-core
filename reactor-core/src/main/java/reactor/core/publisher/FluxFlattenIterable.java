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

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFlattenIterable<T, R> extends FluxSource<T, R> implements Fuseable {

	final Function<? super T, ? extends Iterable<? extends R>> mapper;

	final int prefetch;

	final Supplier<Queue<T>> queueSupplier;

	FluxFlattenIterable(Publisher<? extends T> source,
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

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Callable) {
			T v;

			try {
				v = ((Callable<T>) source).call();
			}
			catch (Throwable ex) {
				Operators.error(s, Operators.onOperatorError(ex));
				return;
			}

			if (v == null) {
				Operators.complete(s);
				return;
			}

			Iterator<? extends R> it;

			try {
				Iterable<? extends R> iter = mapper.apply(v);

				it = iter.iterator();
			}
			catch (Throwable ex) {
				Operators.error(s, Operators.onOperatorError(ex));
				return;
			}

			FluxIterable.subscribe(s, it);

			return;
		}
		source.subscribe(new FlattenIterableSubscriber<>(s,
				mapper,
				prefetch,
				queueSupplier));
	}

	static final class FlattenIterableSubscriber<T, R>
			implements InnerOperator<T, R>, QueueSubscription<R> {

		final Subscriber<? super R> actual;

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

		Iterator<? extends R> current;

		int consumed;

		int fusionMode;

		FlattenIterableSubscriber(Subscriber<? super R> actual,
				Function<? super T, ? extends Iterable<? extends R>> mapper,
				int prefetch,
				Supplier<Queue<T>> queueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.PREFETCH) return prefetch;
			if (key == IntAttr.BUFFERED) return queue != null ? queue.size() : 0;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super R> actual() {
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

						s.request(prefetch);
						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (fusionMode != Fuseable.ASYNC) {
				if (!queue.offer(t)) {
					onError(Operators.onOperatorError(s,Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL)));
					return;
				}
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				s.cancel();

				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}

		void drainAsync() {
			final Subscriber<? super R> a = actual;
			final Queue<T> q = queue;

			int missed = 1;

			Iterator<? extends R> it = current;

			for (; ; ) {

				if (it == null) {

					if (cancelled) {
						q.clear();
						return;
					}

					Throwable ex = error;
					if (ex != null) {
						ex = Exceptions.terminate(ERROR, this);
						current = null;
						q.clear();
						a.onError(ex);
						return;
					}

					boolean d = done;

					T t;

					t = q.poll();

					boolean empty = t == null;

					if (d && empty) {
						a.onComplete();
						return;
					}

					if (!empty) {
						Iterable<? extends R> iterable;

						boolean b;

						try {
							iterable = mapper.apply(t);

							it = iterable.iterator();

							b = it.hasNext();
						}
						catch (Throwable exc) {
							it = null;
							onError(Operators.onOperatorError(s, exc, t));
							continue;
						}

						if (!b) {
							it = null;
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

				if (it != null) {
					long r = requested;
					long e = 0L;

					while (e != r) {
						if (cancelled) {
							current = null;
							q.clear();
							return;
						}

						Throwable ex = error;
						if (ex != null) {
							ex = Exceptions.terminate(ERROR, this);
							current = null;
							q.clear();
							a.onError(ex);
							return;
						}

						R v;

						try {
							v = Objects.requireNonNull(it.next(),
									"iterator returned null");
						}
						catch (Throwable exc) {
							onError(Operators.onOperatorError(s, exc));
							continue;
						}

						a.onNext(v);

						if (cancelled) {
							current = null;
							q.clear();
							return;
						}

						e++;

						boolean b;

						try {
							b = it.hasNext();
						}
						catch (Throwable exc) {
							onError(Operators.onOperatorError(s, exc));
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
							it = null;
							current = null;
							break;
						}
					}

					if (e == r) {
						if (cancelled) {
							current = null;
							q.clear();
							return;
						}

						Throwable ex = error;
						if (ex != null) {
							ex = Exceptions.terminate(ERROR, this);
							current = null;
							q.clear();
							a.onError(ex);
							return;
						}

						boolean d = done;
						boolean empty = q.isEmpty() && it == null;

						if (d && empty) {
							current = null;
							a.onComplete();
							return;
						}
					}

					if (e != 0L) {
						if (r != Long.MAX_VALUE) {
							REQUESTED.addAndGet(this, -e);
						}
					}

					if (it == null) {
						continue;
					}
				}

				current = it;
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainSync() {
			final Subscriber<? super R> a = actual;
//			final Queue<T> q = queue;

			int missed = 1;

			Iterator<? extends R> it = current;

			for (; ; ) {

				if (it == null) {

					if (cancelled) {
						queue.clear();
						return;
					}

					boolean d = done;

					T t;

					t = queue.poll();

					boolean empty = t == null;

					if (d && empty) {
						a.onComplete();
						return;
					}

					if (!empty) {
						Iterable<? extends R> iterable;

						boolean b;

						try {
							iterable = mapper.apply(t);

							it = iterable.iterator();

							b = it.hasNext();
						}
						catch (Throwable exc) {
							current = null;
							a.onError(Operators.onOperatorError(s, exc, t));
							return;
						}

						if (!b) {
							it = null;
							continue;
						}
					}
				}

				if (it != null) {
					long r = requested;
					long e = 0L;

					while (e != r) {
						if (cancelled) {
							current = null;
							queue.clear();
							return;
						}

						R v;

						try {
							v = Objects.requireNonNull(it.next(),
									"iterator returned null");
						}
						catch (Throwable exc) {
							current = null;
							a.onError(Operators.onOperatorError(s, exc));
							return;
						}

						a.onNext(v);

						if (cancelled) {
							current = null;
							queue.clear();
							return;
						}

						e++;

						boolean b;

						try {
							b = it.hasNext();
						}
						catch (Throwable exc) {
							current = null;
							a.onError(Operators.onOperatorError(s, exc));
							return;
						}

						if (!b) {
							it = null;
							current = null;
							break;
						}
					}

					if (e == r) {
						if (cancelled) {
							current = null;
							queue.clear();
							return;
						}

						boolean d = done;
						boolean empty = queue.isEmpty() && it == null;

						if (d && empty) {
							current = null;
							a.onComplete();
							return;
						}
					}

					if (e != 0L) {
						if (r != Long.MAX_VALUE) {
							REQUESTED.addAndGet(this, -e);
						}
					}

					if (it == null) {
						continue;
					}
				}

				current = it;
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
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
			current = null;
			queue.clear();
		}

		@Override
		public boolean isEmpty() {
			Iterator<? extends R> it = current;
			if (it != null) {
				return !it.hasNext();
			}
			return queue.isEmpty(); // estimate
		}

		@Override
		public R poll() {
			Iterator<? extends R> it = current;
			for (; ; ) {
				if (it == null) {
					T v = queue.poll();
					if (v == null) {
						return null;
					}

					it = mapper.apply(v)
					           .iterator();

					if (!it.hasNext()) {
						continue;
					}
					current = it;
				}
				else if (!it.hasNext()) {
					it = null;
					continue;
				}

				R r = it.next();
				if(r == null){
					throw new NullPointerException("iterator returned null");
				}

				if (!it.hasNext()) {
					current = null;
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

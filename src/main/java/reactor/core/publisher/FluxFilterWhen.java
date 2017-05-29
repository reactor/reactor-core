/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.*;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.concurrent.QueueSupplier;

/**
 * Maps each upstream value into a single {@code true} or {@code false} value provided by
 * a generated Publisher for that input value and emits the input value if the inner
 * Publisher returned {@code true}.
 * <p>
 * Only the first item emitted by the inner Publisher's are considered. If
 * the inner Publisher is empty, no resulting item is generated for that input value.
 *
 * @param <T> the input value type
 *
 * @author David Karnok
 * @author Simon Baslé
 */
//adapted from RxJava2Extensions: https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableFilterAsync.java
class FluxFilterWhen<T> extends FluxSource<T, T> {

	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	final int bufferSize;

	FluxFilterWhen(Publisher<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
			int bufferSize) {
		super(source);
		this.asyncPredicate = asyncPredicate;
		this.bufferSize = bufferSize;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FluxFilterWhenSubscriber<>(s, asyncPredicate, bufferSize));
	}

	static final class FluxFilterWhenSubscriber<T> implements InnerOperator<T, T> {

		final Subscriber<? super T>                             actual;
		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;
		final int                                               bufferSize;
		final AtomicReferenceArray<T>                           toFilter;

		int          consumed;
		long         consumerIndex;
		Boolean      innerResult;
		long         producerIndex;
		Subscription upstream;

		volatile boolean         cancelled;
		volatile FilterWhenInner current;
		volatile boolean         done;
		volatile Throwable       error;
		volatile long            requested;
		volatile int             state;
		volatile int             wip;

		static final AtomicReferenceFieldUpdater<FluxFilterWhenSubscriber, Throwable>      ERROR     =
				AtomicReferenceFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, Throwable.class, "error");
		static final AtomicLongFieldUpdater<FluxFilterWhenSubscriber>                      REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, "requested");
		static final AtomicIntegerFieldUpdater<FluxFilterWhenSubscriber>                   WIP       =
				AtomicIntegerFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, "wip");
		static final AtomicReferenceFieldUpdater<FluxFilterWhenSubscriber, FilterWhenInner>CURRENT   =
				AtomicReferenceFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, FilterWhenInner.class, "current");

		static final FilterWhenInner INNER_CANCELLED = new FilterWhenInner(null, false);

		static final int STATE_FRESH   = 0;
		static final int STATE_RUNNING = 1;
		static final int STATE_RESULT  = 2;

		FluxFilterWhenSubscriber(Subscriber<? super T> actual,
				Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
				int bufferSize) {
			this.toFilter = new AtomicReferenceArray<>(QueueSupplier.ceilingNextPowerOfTwo(bufferSize));
			this.actual = actual;
			this.asyncPredicate = asyncPredicate;
			this.bufferSize = bufferSize;
		}

		@Override
		public void onNext(T t) {
			long pi = producerIndex;
			int m = toFilter.length() - 1;

			int offset = (int)pi & m;
			toFilter.lazySet(offset, t);
			producerIndex = pi + 1;
			drain();
		}

		@Override
		public void onError(Throwable t) {
			ERROR.set(this, t);
			done = true;
			drain();
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
				upstream.cancel();
				cancelInner();
				if (WIP.getAndIncrement(this) == 0) {
					clear();
				}
			}
		}

		void cancelInner() {
			FilterWhenInner a = CURRENT.get(this);
			if (a != INNER_CANCELLED) {
				a = CURRENT.getAndSet(this, INNER_CANCELLED);
				if (a != null && a != INNER_CANCELLED) {
					a.cancel();
				}
			}
		}

		void clear() {
			int n = toFilter.length();
			for (int i = 0; i < n; i++) {
				toFilter.lazySet(i, null);
			}
			innerResult = null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(upstream, s)) {
				upstream = s;
				actual.onSubscribe(this);
				s.request(bufferSize);
			}
		}

		@SuppressWarnings("unchecked")
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			int limit = bufferSize - (bufferSize >> 2);
			long ci = consumerIndex;
			int f = consumed;
			int m = toFilter.length() - 1;
			Subscriber<? super T> a = actual;

			for (;;) {
				long r = requested;

				while (ci != r) {
					if (cancelled) {
						clear();
						return;
					}

					boolean d = done;

					int offset = (int)ci & m;
					T t = toFilter.get(offset);
					boolean empty = t == null;

					if (d && empty) {
						Throwable ex = Exceptions.terminate(ERROR, this);
						if (ex == null) {
							a.onComplete();
						} else {
							a.onError(ex);
						}
						return;
					}

					if (empty) {
						break;
					}

					int s = state;
					if (s == STATE_FRESH) {
						Publisher<Boolean> p;

						try {
							p = Objects.requireNonNull(asyncPredicate.apply(t), "The asyncPredicate returned a null value");
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							Exceptions.addThrowable(ERROR, this, ex);
							p = null;
						}

						if (p != null) {
							if (p instanceof Callable) {
								Boolean u;

								try {
									u = ((Callable<Boolean>)p).call();
								} catch (Throwable ex) {
									Exceptions.throwIfFatal(ex);
									Exceptions.addThrowable(ERROR, this, ex);
									u = null;
								}

								if (u != null && u) {
									a.onNext(t);
								}
							} else {
								FilterWhenInner inner = new FilterWhenInner(this, !(p instanceof Mono));
								if (CURRENT.compareAndSet(this,null, inner)) {
									state = STATE_RUNNING;
									p.subscribe(inner);
									break;
								}
							}
						}

						toFilter.lazySet(offset, null);
						ci++;
						if (++f == limit) {
							f = 0;
							upstream.request(limit);
						}
					} else
					if (s == STATE_RESULT) {
						Boolean u = innerResult;
						innerResult = null;

						if (u != null && u) {
							a.onNext(t);
						}

						toFilter.lazySet(offset, null);
						ci++;
						if (++f == limit) {
							f = 0;
							upstream.request(limit);
						}
						state = STATE_FRESH;
					} else {
						break;
					}
				}

				if (ci == r) {
					if (cancelled) {
						clear();
						return;
					}

					boolean d = done;

					int offset = (int)ci & m;
					T t = toFilter.get(offset);
					boolean empty = t == null;

					if (d && empty) {
						Throwable ex = Exceptions.terminate(ERROR, this);
						if (ex == null) {
							a.onComplete();
						} else {
							a.onError(ex);
						}
						return;
					}
				}

				int w = wip;
				if (missed == w) {
					consumed = f;
					consumerIndex = ci;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		void clearCurrent() {
			FilterWhenInner c = current;
			if (c != INNER_CANCELLED) {
				CURRENT.compareAndSet(this, c, null);
			}
		}

		public void innerResult(Boolean item) {
			innerResult = item;
			state = STATE_RESULT;
			clearCurrent();
			drain();
		}

		public void innerError(Throwable ex) {
			Exceptions.addThrowable(ERROR, this, ex);
			state = STATE_RESULT;
			clearCurrent();
			drain();
		}

		public void innerComplete() {
			state = STATE_RESULT;
			clearCurrent();
			drain();
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return upstream;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == ThrowableAttr.ERROR) //FIXME ERROR is often reset by Exceptions.terminate :(
				return error;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == IntAttr.CAPACITY) return toFilter.length();
			if (key == IntAttr.BUFFERED) return (int) (producerIndex - consumerIndex);
			if (key == IntAttr.PREFETCH) return bufferSize;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			FilterWhenInner c = current;
			return c == null ? Stream.empty() : Stream.of(c);
		}
	}

	static final class FilterWhenInner implements InnerConsumer<Boolean> {

		final FluxFilterWhenSubscriber<?> parent;
		final boolean                     cancelOnNext;

		boolean done;

		volatile Subscription sub;

		static final AtomicReferenceFieldUpdater<FilterWhenInner, Subscription> SUB =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenInner.class, Subscription.class, "sub");

		FilterWhenInner(FluxFilterWhenSubscriber<?> parent, boolean cancelOnNext) {
			this.parent = parent;
			this.cancelOnNext = cancelOnNext;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUB, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Boolean t) {
			if (!done) {
				if (cancelOnNext) {
					sub.cancel();
				}
				done = true;
				parent.innerResult(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!done) {
				done = true;
				parent.innerError(t);
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				parent.innerComplete();
			}
		}

		void cancel() {
			Operators.terminate(SUB, this);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return parent;
			if (key == ScannableAttr.ACTUAL) return sub;
			if (key == BooleanAttr.CANCELLED) return sub == Operators.cancelledSubscription();
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return done ? 0L : 1L;

			return null;
		}
	}
}

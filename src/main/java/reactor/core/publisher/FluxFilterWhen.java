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

import org.reactivestreams.*;
import reactor.core.Exceptions;
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
public class FluxFilterWhen<T> extends FluxSource<T, T> {

	final Publisher<T> source;

	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	final int bufferSize;

	FluxFilterWhen(Publisher<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
			int bufferSize) {
		super(source);
		this.source = source;
		this.asyncPredicate = asyncPredicate;
		this.bufferSize = bufferSize;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FilterWhenSubscriber<>(s, asyncPredicate, bufferSize));
	}

	static final class FilterWhenSubscriber<T> implements Subscriber<T>, Subscription {

		final Subscriber<? super T>                             actual;
		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;
		final int                                               bufferSize;
		final AtomicReferenceArray<T>                           toFilter;

		int          consumed;
		long         consumerIndex;
		Boolean      innerResult;
		long         producerIndex;
		Subscription upstream;

		volatile boolean               cancelled;
		volatile FilterInnerSubscriber current;
		volatile boolean               done;
		volatile Throwable             error;
		volatile long                  requested;
		volatile int                   state;
		volatile int                   wip;

		static final AtomicReferenceFieldUpdater<FilterWhenSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenSubscriber.class, Throwable.class, "error");
		static final AtomicLongFieldUpdater<FilterWhenSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FilterWhenSubscriber.class, "requested");
		static final AtomicIntegerFieldUpdater<FilterWhenSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FilterWhenSubscriber.class, "wip");
		static final AtomicReferenceFieldUpdater<FilterWhenSubscriber, FilterInnerSubscriber> CURRENT =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenSubscriber.class, FilterInnerSubscriber.class, "current");

		static final FilterInnerSubscriber INNER_CANCELLED = new FilterInnerSubscriber(null, false);

		static final int STATE_FRESH   = 0;
		static final int STATE_RUNNING = 1;
		static final int STATE_RESULT  = 2;

		FilterWhenSubscriber(Subscriber<? super T> actual,
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
			FilterInnerSubscriber a = CURRENT.get(this);
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
								FilterInnerSubscriber inner = new FilterInnerSubscriber(this, !(p instanceof Mono));
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
			FilterInnerSubscriber c = current;
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
	}

	static final class FilterInnerSubscriber implements Subscriber<Boolean> {

		final FilterWhenSubscriber<?> parent;
		final boolean                 cancelOnNext;

		boolean done;

		volatile Subscription sub;
		static final AtomicReferenceFieldUpdater<FilterInnerSubscriber, Subscription> SUB =
				AtomicReferenceFieldUpdater.newUpdater(FilterInnerSubscriber.class, Subscription.class, "sub");

		FilterInnerSubscriber(FilterWhenSubscriber<?> parent, boolean cancelOnNext) {
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
	}
}

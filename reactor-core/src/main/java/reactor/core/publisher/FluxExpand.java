/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A {@link Flux} that emits items from upstream and recursively expand them into
 * inner sequences that are also replayed. The type of recursion is driven by the
 * {@code breadthFirst} parameter.
 *
 * @param <T>
 *
 * @author David Karnok
 * @author Simon Baslé
 */
//adapted from RxJava2Extensions: https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableExpand.java
final class FluxExpand<T> extends InternalFluxOperator<T, T> {

	final boolean                                               breadthFirst;
	final Function<? super T, ? extends Publisher<? extends T>> expander;
	final int                                                   capacityHint;

	FluxExpand(Flux<T> source,
			Function<? super T, ? extends Publisher<? extends T>> expander,
			boolean breadthFirst, int capacityHint) {
		super(source);
		this.expander = expander;
		this.breadthFirst = breadthFirst;
		this.capacityHint = capacityHint;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> s) {
		if (breadthFirst) {
			ExpandBreathSubscriber<T> parent =
					new ExpandBreathSubscriber<>(s, expander, capacityHint);
			parent.queue.offer(source);
			s.onSubscribe(parent);
			parent.drainQueue();
		}
		else {
			ExpandDepthSubscription<T> parent =
					new ExpandDepthSubscription<>(s, expander, capacityHint);
			parent.source = source;
			s.onSubscribe(parent);
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ExpandBreathSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Function<? super T, ? extends Publisher<? extends T>> expander;
		final Queue<Publisher<? extends T>>                         queue;

		volatile boolean active;
		volatile int     wip;

		static final AtomicIntegerFieldUpdater<ExpandBreathSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ExpandBreathSubscriber.class, "wip");

		long produced;

		ExpandBreathSubscriber(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<? extends T>> expander,
				int capacityHint) {
			super(actual);
			this.expander = expander;
			this.queue = Queues.<Publisher<? extends T>>unbounded(capacityHint).get();
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public void onNext(T t) {
			produced++;
			actual.onNext(t);

			Publisher<? extends T> p;
			try {
				p = Objects.requireNonNull(expander.apply(t),
						"The expander returned a null Publisher");
			}
			catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				super.cancel();
				actual.onError(ex);
				drainQueue();
				return;
			}

			queue.offer(p);
		}

		@Override
		public void onError(Throwable t) {
			set(Operators.cancelledSubscription());
			super.cancel();
			actual.onError(t);
			drainQueue();
		}

		@Override
		public void onComplete() {
			active = false;
			drainQueue();
		}

		@Override
		public void cancel() {
			super.cancel();
			drainQueue();
		}

		void drainQueue() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					Queue<Publisher<? extends T>> q = queue;
					if (isCancelled()) {
						q.clear();
					}
					else {
						if (!active) {
							if (q.isEmpty()) {
								set(Operators.cancelledSubscription());
								super.cancel();
								actual.onComplete();
							}
							else {
								Publisher<? extends T> p = q.poll();
								long c = produced;
								if (c != 0L) {
									produced = 0L;
									produced(c);

								}
								active = true;
								p.subscribe(this);
							}
						}
					}
				}
				while (WIP.decrementAndGet(this) != 0);
			}
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}
	}

	static final class ExpandDepthSubscription<T> implements InnerProducer<T> {

		final CoreSubscriber<? super T>                             actual;
		final Function<? super T, ? extends Publisher<? extends T>> expander;

		volatile Throwable error;
		static final AtomicReferenceFieldUpdater<ExpandDepthSubscription, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ExpandDepthSubscription.class, Throwable.class, "error");

		volatile int       active;
		static final AtomicIntegerFieldUpdater<ExpandDepthSubscription> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(ExpandDepthSubscription.class, "active");

		volatile long      requested;
		static final AtomicLongFieldUpdater<ExpandDepthSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ExpandDepthSubscription.class, "requested");

		volatile Object    current;
		static final AtomicReferenceFieldUpdater<ExpandDepthSubscription, Object> CURRENT =
				AtomicReferenceFieldUpdater.newUpdater(ExpandDepthSubscription.class, Object.class, "current");

		volatile int wip;
		static final AtomicIntegerFieldUpdater<ExpandDepthSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ExpandDepthSubscription.class, "wip");

		Deque<ExpandDepthSubscriber<T>> subscriptionStack;

		volatile boolean cancelled;

		CorePublisher<? extends T> source;
		long                       consumed;

		ExpandDepthSubscription(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<? extends T>> expander,
				int capacityHint) {
			this.actual = actual;
			this.expander = expander;
			this.subscriptionStack = new ArrayDeque<>(capacityHint);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drainQueue();
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				Deque<ExpandDepthSubscriber<T>> q;
				synchronized (this) {
					q = subscriptionStack;
					subscriptionStack = null;
				}

				if (q != null) {
					while (!q.isEmpty()) {
						q.poll().dispose();
					}
				}

				Object o = CURRENT.getAndSet(this, this);
				if (o != this && o != null) {
					((ExpandDepthSubscriber) o).dispose();
				}
			}
		}

		@Nullable
		ExpandDepthSubscriber<T> pop() {
			synchronized (this) {
				Deque<ExpandDepthSubscriber<T>> q = subscriptionStack;
				return q != null ? q.pollFirst() : null;
			}
		}

		boolean push(ExpandDepthSubscriber<T> subscriber) {
			synchronized (this) {
				Deque<ExpandDepthSubscriber<T>> q = subscriptionStack;
				if (q != null) {
					q.offerFirst(subscriber);
					return true;
				}
				return false;
			}
		}

		boolean setCurrent(ExpandDepthSubscriber<T> inner) {
			for (;;) {
				Object o = CURRENT.get(this);
				if (o == this) {
					inner.dispose();
					return false;
				}
				if (CURRENT.compareAndSet(this, o, inner)) {
					return true;
				}
			}
		}

		void drainQueue() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			Subscriber<? super T> a = actual;
			long e = consumed;

			for (;;) {
				Object o = current;
				if (cancelled || o == this) {
					source = null;
					return;
				}

				@SuppressWarnings("unchecked")
				ExpandDepthSubscriber<T> curr = (ExpandDepthSubscriber<T>)o;
				Publisher<? extends T> p = source;

				if (curr == null && p != null) {
					source = null;
					ACTIVE.getAndIncrement(this);

					ExpandDepthSubscriber<T> eds = new ExpandDepthSubscriber<>(this);
					if (setCurrent(eds)) {
						p.subscribe(eds);
					}
					else {
						return;
					}
				}
				else if (curr == null) {
					return;
				}
				else {
					boolean currentDone = curr.done;
					T v = curr.value;

					boolean newSource = false;
					if (v != null && e != requested) {
						curr.value = null;
						a.onNext(v);
						e++;

						try {
							p = Objects.requireNonNull(expander.apply(v),
									"The expander returned a null Publisher");
						}
						catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							p = null;
							curr.dispose();
							curr.done = true;
							currentDone = true;
							v = null;
							Exceptions.addThrowable(ERROR, this, ex);
						}

						if (p != null) {
							if (push(curr)) {
								ACTIVE.getAndIncrement(this);
								curr = new ExpandDepthSubscriber<>(this);
								if (setCurrent(curr)) {
									p.subscribe(curr);
									newSource = true;
								}
								else {
									return;
								}
							}
						}
					}

					if (!newSource) {
						if (currentDone && v == null) {
							if (ACTIVE.decrementAndGet(this) == 0) {
								Throwable ex = Exceptions.terminate(ERROR, this);
								if (ex != null) {
									a.onError(ex);
								}
								else {
									a.onComplete();
								}
								return;
							}
							curr = pop();
							if (curr != null && setCurrent(curr)) {
								curr.requestOne();
								continue;
							}
							else {
								return;
							}
						}
					}
				}
				int w = wip;
				if (missed == w) {
					consumed = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		void innerNext() {
			drainQueue();
		}

		void innerError(ExpandDepthSubscriber inner, Throwable t) {
			Exceptions.addThrowable(ERROR, this, t);
			inner.done = true;
			drainQueue();
		}

		void innerComplete(ExpandDepthSubscriber inner) {
			inner.done = true;
			drainQueue();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return this.requested;
			if (key == Attr.ERROR) return this.error;

			return InnerProducer.super.scanUnsafe(key);
		}
	}

	static final class ExpandDepthSubscriber<T> implements InnerConsumer<T> {

		ExpandDepthSubscription<T> parent;

		volatile boolean done;
		volatile T       value;

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<ExpandDepthSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ExpandDepthSubscriber.class, Subscription.class, "s");

		ExpandDepthSubscriber(ExpandDepthSubscription<T> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(1);
			}
		}

		@Override
		public void onNext(T t) {
			if (s != Operators.cancelledSubscription()) {
				value = t;
				parent.innerNext();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (s != Operators.cancelledSubscription()) {
				parent.innerError(this, t);
			}
		}

		@Override
		public void onComplete() {
			if (s != Operators.cancelledSubscription()) {
				parent.innerComplete(this);
			}
		}

		void requestOne() {
			s.request(1);
		}

		void dispose() {
			Operators.terminate(S, this);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent.actual;
			if (key == Attr.TERMINATED) return this.done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public Context currentContext() {
			return parent.actual().currentContext();
		}
	}
}

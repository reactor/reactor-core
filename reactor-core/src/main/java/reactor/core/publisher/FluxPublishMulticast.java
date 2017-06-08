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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;
import javax.annotation.Nullable;

/**
 * Shares a sequence for the duration of a function that may transform it and
 * consume it as many times as necessary without causing multiple subscriptions
 * to the upstream.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublishMulticast<T, R> extends FluxOperator<T, R> implements Fuseable {

	final Function<? super Flux<T>, ? extends Publisher<? extends R>> transform;

	final Supplier<? extends Queue<T>> queueSupplier;

	final int prefetch;

	FluxPublishMulticast(ContextualPublisher<? extends T> source,
			Function<? super Flux<T>, ? extends Publisher<? extends R>> transform,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch < 1) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.prefetch = prefetch;
		this.transform = Objects.requireNonNull(transform, "transform");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super R> s, Context ctx) {

		FluxPublishMulticaster<T, R> multicast =
				new FluxPublishMulticaster<>(prefetch, queueSupplier, ctx);

		Publisher<? extends R> out;

		try {
			out = Objects.requireNonNull(transform.apply(multicast),
					"The transform returned a null Publisher");
		}
		catch (Throwable ex) {
			Operators.error(s, Operators.onOperatorError(ex));
			return;
		}

		if (out instanceof Fuseable) {
			out.subscribe(new CancelFuseableMulticaster<>(s, multicast));
		}
		else {
			out.subscribe(new CancelMulticaster<>(s, multicast));
		}

		source.subscribe(multicast, ctx);
	}

	static final class FluxPublishMulticaster<T, R> extends Flux<T>
			implements InnerConsumer<T> {

		final int limit;

		final int prefetch;

		final Supplier<? extends Queue<T>> queueSupplier;

		Queue<T> queue;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FluxPublishMulticaster, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(FluxPublishMulticaster.class,
						Subscription.class,
						"s");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FluxPublishMulticaster> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FluxPublishMulticaster.class, "wip");

		volatile PublishMulticastInner<T>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FluxPublishMulticaster, PublishMulticastInner[]>
				SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(
				FluxPublishMulticaster.class,
				PublishMulticastInner[].class,
				"subscribers");

		@SuppressWarnings("rawtypes")
		static final PublishMulticastInner[] EMPTY = new PublishMulticastInner[0];

		@SuppressWarnings("rawtypes")
		static final PublishMulticastInner[] TERMINATED =
				new PublishMulticastInner[0];

		volatile boolean done;

		volatile boolean connected;

		volatile boolean cancelled;

		Throwable error;

		final Context context;

		int produced;

		int sourceMode;

		@SuppressWarnings("unchecked")
		FluxPublishMulticaster(int prefetch, Supplier<? extends Queue<T>>
				queueSupplier, Context ctx) {
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
			this.queueSupplier = queueSupplier;
			this.subscribers = EMPTY;
			this.context = ctx;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.PREFETCH) return prefetch;
			if (key == IntAttr.BUFFERED) return queue != null ? queue.size() : 0;

			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public Context currentContext() {
			return context;
		}

		@Override
		public void subscribe(Subscriber<? super T> s, Context ctx) {
			PublishMulticastInner<T> pcs = new PublishMulticastInner<>(this, s);
			s.onSubscribe(pcs);

			if (add(pcs)) {
				ContextRelay.set(s, context);
				if (pcs.once != 0) {
					removeAndDrain(pcs);
				}
				else {
					drain();
				}
			}
			else {
				Throwable ex = error;
				if (ex != null) {
					s.onError(ex);
				}
				else {
					s.onComplete();
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> qs =
							(QueueSubscription<T>) s;

					int m = qs.requestFusion(Fuseable.ANY);
					if (m == Fuseable.SYNC) {
						sourceMode = m;

						queue = qs;
						done = true;
						connected = true;

						drain();

						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = m;

						queue = qs;
						connected = true;

						s.request(prefetch);

						return;
					}
				}

				queue = queueSupplier.get();
				connected = true;

				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			if (sourceMode != Fuseable.ASYNC) {
				if (!queue.offer(t)) {
					onError(Operators.onOperatorError(s,
							Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
							t));
					return;
				}
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (sourceMode == Fuseable.SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		@SuppressWarnings("unchecked")
		void drainSync() {
			int missed = 1;

			for (; ; ) {

				if (connected) {

					if (cancelled) {
						queue.clear();
						return;
					}

					final Queue<T> queue = this.queue;

					PublishMulticastInner<T>[] a = subscribers;
					int n = a.length;

					if (n != 0) {

						long r = Long.MAX_VALUE;

						for (int i = 0; i < n; i++) {
							r = Math.min(r, a[i].requested);
						}

						long e = 0L;

						while (e != r) {

							if (cancelled) {
								queue.clear();
								return;
							}

							T v;

							try {
								v = queue.poll();
							}
							catch (Throwable ex) {
								error = Operators.onOperatorError(s, ex);
								queue.clear();
								a = SUBSCRIBERS.getAndSet(this, TERMINATED);
								n = a.length;
								for (int i = 0; i < n; i++) {
									a[i].actual.onError(ex);
								}
								return;
							}

							if (v == null) {
								a = SUBSCRIBERS.getAndSet(this, TERMINATED);
								n = a.length;
								for (int i = 0; i < n; i++) {
									a[i].actual.onComplete();
								}
								return;
							}

							for (int i = 0; i < n; i++) {
								a[i].actual.onNext(v);
							}

							e++;
						}

						if (cancelled) {
							queue.clear();
							return;
						}
						if (queue.isEmpty()) {
							a = SUBSCRIBERS.getAndSet(this, TERMINATED);
							n = a.length;
							for (int i = 0; i < n; i++) {
								a[i].actual.onComplete();
							}
							return;
						}

						if (e != 0L) {
							for (int i = 0; i < n; i++) {
								a[i].produced(e);
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

		@SuppressWarnings("unchecked")
		void drainAsync() {
			int missed = 1;

			int p = produced;

			for (; ; ) {

				if (connected) {
					if (cancelled) {
						queue.clear();
						return;
					}

					final Queue<T> queue = this.queue;

					PublishMulticastInner<T>[] a = subscribers;
					int n = a.length;

					if (n != 0) {

						long r = Long.MAX_VALUE;

						for (int i = 0; i < n; i++) {
							r = Math.min(r, a[i].requested);
						}

						long e = 0L;

						while (e != r) {
							if (cancelled) {
								queue.clear();
								return;
							}

							boolean d = done;

							T v;

							try {
								v = queue.poll();
							}
							catch (Throwable ex) {
								queue.clear();
								error = Operators.onOperatorError(s, ex);
								a = SUBSCRIBERS.getAndSet(this, TERMINATED);
								n = a.length;
								for (int i = 0; i < n; i++) {
									a[i].actual.onError(ex);
								}
								return;
							}

							boolean empty = v == null;

							if (d) {
								Throwable ex = error;
								if (ex != null) {
									queue.clear();
									a = SUBSCRIBERS.getAndSet(this, TERMINATED);
									n = a.length;
									for (int i = 0; i < n; i++) {
										a[i].actual.onError(ex);
									}
									return;
								}
								else if (empty) {
									a = SUBSCRIBERS.getAndSet(this, TERMINATED);
									n = a.length;
									for (int i = 0; i < n; i++) {
										a[i].actual.onComplete();
									}
									return;
								}
							}

							if (empty) {
								break;
							}

							for (int i = 0; i < n; i++) {
								a[i].actual.onNext(v);
							}

							e++;

							if (++p == limit) {
								s.request(p);
								p = 0;
							}
						}

						if (e == r) {
							if (cancelled) {
								queue.clear();
								return;
							}

							boolean d = done;

							if (d) {
								Throwable ex = error;
								if (ex != null) {
									queue.clear();
									a = SUBSCRIBERS.getAndSet(this, TERMINATED);
									n = a.length;
									for (int i = 0; i < n; i++) {
										a[i].actual.onError(ex);
									}
									return;
								}
								else if (queue.isEmpty()) {
									a = SUBSCRIBERS.getAndSet(this, TERMINATED);
									n = a.length;
									for (int i = 0; i < n; i++) {
										a[i].actual.onComplete();
									}
									return;
								}
							}

						}

						if (e != 0L) {
							for (int i = 0; i < n; i++) {
								a[i].produced(e);
							}
						}
					}

				}

				produced = p;

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean add(PublishMulticastInner<T> s) {
			for (; ; ) {
				PublishMulticastInner<T>[] a = subscribers;

				if (a == TERMINATED) {
					return false;
				}

				int n = a.length;

				@SuppressWarnings("unchecked") PublishMulticastInner<T>[] b =
						new PublishMulticastInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = s;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		@SuppressWarnings("unchecked")
		void removeAndDrain(PublishMulticastInner<T> s) {
			for (; ; ) {
				PublishMulticastInner<T>[] a = subscribers;

				if (a == TERMINATED || a == EMPTY) {
					return;
				}

				int n = a.length;
				int j = -1;

				for (int i = 0; i < n; i++) {
					if (a[i] == s) {
						j = i;
						break;
					}
				}

				if (j < 0) {
					return;
				}

				PublishMulticastInner<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new PublishMulticastInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					drain();
					return;
				}
			}
		}

		void cancel() {
			if (!cancelled) {
				cancelled = true;
				terminate();
			}
		}

		@SuppressWarnings("unchecked")
		boolean terminate() {
			if(Operators.replace(S, this, Operators.cancelledSubscription())) {
				subscribers = TERMINATED;
				if (WIP.getAndIncrement(this) == 0) {
					if (connected) {
						queue.clear();
					}
				}
				return true;
			}
			return false;
		}
	}

	static final class PublishMulticastInner<T> implements InnerProducer<T> {

		final FluxPublishMulticaster<T, ?> parent;

		final Subscriber<? super T> actual;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishMulticastInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishMulticastInner.class,
						"requested");

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishMulticastInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(PublishMulticastInner.class,
						"once");

		PublishMulticastInner(FluxPublishMulticaster<T, ?> parent,
				Subscriber<? super T> actual) {
			this.parent = parent;
			this.actual = actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == ScannableAttr.PARENT) return parent;
			if (key == BooleanAttr.CANCELLED) return once == 1;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				parent.drain();
			}
		}

		@Override
		public void cancel() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				parent.removeAndDrain(this);
			}
		}

		void produced(long n) {
			if (requested != Long.MAX_VALUE) {
				REQUESTED.addAndGet(this, -n);
			}
		}
	}

	static final class CancelMulticaster<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final Subscriber<? super T> actual;

		final FluxPublishMulticaster<?, ?> parent;

		Subscription s;

		CancelMulticaster(Subscriber<? super T> actual,
				FluxPublishMulticaster<?, ?> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			parent.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if(!parent.terminate()){
				Operators.onErrorDropped(t);
				return;
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if(parent.terminate()) {
				actual.onComplete();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return NONE;
		}

		@Override
		public void clear() {
			// should not be called because fusion is always rejected
		}

		@Override
		public boolean isEmpty() {
			// should not be called because fusion is always rejected
			return false;
		}

		@Override
		public int size() {
			// should not be called because fusion is always rejected
			return 0;
		}

		@Override
		@Nullable
		public T poll() {
			// should not be called because fusion is always rejected
			return null;
		}
	}

	static final class CancelFuseableMulticaster<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final Subscriber<? super T> actual;

		final FluxPublishMulticaster<?, ?> parent;

		QueueSubscription<T> s;

		CancelFuseableMulticaster(Subscriber<? super T> actual,
				FluxPublishMulticaster<?, ?> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			parent.cancel();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if(Operators.validate(this.s, s)) {
				this.s = Operators.as(s);
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if(!parent.terminate()){
				Operators.onErrorDropped(t);
				return;
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if(parent.terminate()){
				actual.onComplete();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return s.requestFusion(requestedMode);
		}

		@Override
		@Nullable
		public T poll() {
			return s.poll();
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public int size() {
			return s.size();
		}

		@Override
		public void clear() {
			s.clear();
		}
	}

}

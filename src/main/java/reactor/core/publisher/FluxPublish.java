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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;

/**
 * A connectable publisher which shares an underlying source and dispatches source values
 * to subscribers in a backpressure-aware manner.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublish<T> extends ConnectableFlux<T> implements Scannable {

	/**
	 * The source observable.
	 */
	final Flux<? extends T> source;

	/**
	 * The size of the prefetch buffer.
	 */
	final int prefetch;

	final Supplier<? extends Queue<T>> queueSupplier;

	volatile PublishSubscriber<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxPublish, PublishSubscriber> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxPublish.class,
					PublishSubscriber.class,
					"connection");

	FluxPublish(Flux<? extends T> source,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("bufferSize > 0 required but it was " + prefetch);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		boolean doConnect;
		PublishSubscriber<T> s;
		for (; ; ) {
			s = connection;
			if (s == null || s.isTerminated()) {
				PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);

				if (!CONNECTION.compareAndSet(this, s, u)) {
					continue;
				}

				s = u;
			}

			doConnect = s.tryConnect();
			break;
		}

		cancelSupport.accept(s);
		if (doConnect) {
			source.subscribe(s);
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		PublishInner<T> inner = new PublishInner<>(s);
		s.onSubscribe(inner);
		for (; ; ) {
			if (inner.isCancelled()) {
				break;
			}

			PublishSubscriber<T> c = connection;
			if (c == null || c.isTerminated()) {
				PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}

				c = u;
			}

			if (c.trySubscribe(inner)) {
				break;
			}
		}
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public Object scan(Attr key) {
		switch (key){
			case PREFETCH:
				return getPrefetch();
			case PARENT:
				return source;
		}
		return null;
	}

	static final class PublishSubscriber<T>
			implements InnerConsumer<T>, Disposable {

		final int prefetch;

		final FluxPublish<T> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(PublishSubscriber.class,
						Subscription.class,
						"s");

		volatile PublishInner<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishSubscriber.class, "wip");

		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishSubscriber> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(PublishSubscriber.class,
						"connected");

		@SuppressWarnings("rawtypes")
		static final PublishInner[] EMPTY      = new PublishInner[0];
		@SuppressWarnings("rawtypes")
		static final PublishInner[] TERMINATED = new PublishInner[0];

		volatile Queue<T> queue;

		int sourceMode;

		volatile boolean   done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(PublishSubscriber.class,
						Throwable.class,
						"error");

		volatile boolean cancelled;

		@SuppressWarnings("unchecked")
		PublishSubscriber(int prefetch, FluxPublish<T> parent) {
			this.prefetch = prefetch;
			this.parent = parent;
			this.subscribers = EMPTY;
		}

		boolean isTerminated(){
			return subscribers == TERMINATED;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
							(Fuseable.QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY);
					if (m == Fuseable.SYNC) {
						sourceMode = m;
						queue = f;
						done = true;
						drain();
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = m;
						queue = f;
						s.request(prefetch);
						return;
					}
				}

				queue = parent.queueSupplier.get();

				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				if (t != null) {
					Operators.onNextDropped(t);
				}
				return;
			}
			if (sourceMode == Fuseable.ASYNC) {
				drain();
				return;
			}

			if (!queue.offer(t)) {
				Throwable ex = Operators.onOperatorError(s,
						Exceptions.failWithOverflow("Queue is full?!"),
						t);
				if (!Exceptions.addThrowable(ERROR, this, ex)) {
					Operators.onErrorDropped(ex);
					return;
				}
				done = true;
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
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
		public void dispose() {
			if (cancelled) {
				return;
			}
			if (Operators.terminate(S, this)) {
				cancelled = true;
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}
				disconnectAction();
			}
		}

		void disconnectAction() {
			queue.clear();
			CancellationException ex = new CancellationException("Disconnected");
			for (PublishInner<T> inner : terminate()) {
				inner.actual.onError(ex);
			}
		}

		boolean add(PublishInner<T> inner) {
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				PublishInner<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;

				@SuppressWarnings("unchecked") PublishInner<T>[] b =
						new PublishInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;

				subscribers = b;
				return true;
			}
		}

		@SuppressWarnings("unchecked")
		void remove(PublishInner<T> inner) {
			PublishInner<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			synchronized (this) {
				a = subscribers;
				if (a == TERMINATED || a == EMPTY) {
					return;
				}

				int j = -1;
				int n = a.length;
				for (int i = 0; i < n; i++) {
					if (a[i] == inner) {
						j = i;
						break;
					}
				}
				if (j < 0) {
					return;
				}

				PublishInner<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new PublishInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}

				subscribers = b;
			}
		}

		@SuppressWarnings("unchecked")
		PublishInner<T>[] terminate() {
			PublishInner<T>[] a;
			for (; ; ) {
				a = subscribers;
				if (a == TERMINATED) {
					return a;
				}
				synchronized (this) {
					a = subscribers;
					if (a != TERMINATED) {
						subscribers = TERMINATED;
					}
					return a;
				}
			}
		}

		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}

		boolean trySubscribe(PublishInner<T> inner) {
			if (add(inner)) {
				if (inner.isCancelled()) {
					remove(inner);
				}
				else {
					inner.parent = this;
					drain();
				}
				return true;
			}
			return false;
		}

		void replenish(long n) {
			if (sourceMode != Fuseable.SYNC) {
				s.request(n);
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {

				boolean d = done;

				Queue<T> q = queue;

				boolean empty = q == null || q.isEmpty();

				if (checkTerminated(d, empty)) {
					return;
				}

				if (!empty) {
					PublishInner<T>[] a = subscribers;
					long maxRequested = Long.MAX_VALUE;

					int len = a.length;
					int cancel = 0;

					for (PublishInner<T> inner : a) {
						long r = inner.requested;
						if (r >= 0L) {
							maxRequested = Math.min(maxRequested, r);
						}
						else { //Long.MIN == PublishInner.CANCEL_REQUEST
							cancel++;
						}
					}

					if (len == cancel) {
						T v;

						try {
							v = q.poll();
						}
						catch (Throwable ex) {
							Exceptions.addThrowable(ERROR,
									this,
									Operators.onOperatorError(s, ex));
							d = true;
							v = null;
						}
						if (checkTerminated(d, v == null)) {
							return;
						}
						replenish(1);
						continue;
					}

					int e = 0;

					while (e < maxRequested && cancel != Integer.MIN_VALUE) {
						d = done;
						T v;

						try {
							v = q.poll();
						}
						catch (Throwable ex) {
							Exceptions.addThrowable(ERROR,
									this,
									Operators.onOperatorError(s, ex));
							d = true;
							v = null;
						}

						empty = v == null;

						if (checkTerminated(d, empty)) {
							return;
						}

						if (empty) {
							break;
						}

						for (PublishInner<T> inner : a) {
							inner.actual.onNext(v);
							if(inner.produced(1) == PublishInner.CANCEL_REQUEST){
								cancel = Integer.MIN_VALUE;
							}
						}

						e++;
					}

					if (e != 0) {
						replenish(e);
					}

					if (maxRequested != 0L && !empty) {
						continue;
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty) {
			if (cancelled) {
				disconnectAction();
				return true;
			}
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					CONNECTION.compareAndSet(parent, this, null);
					e = Exceptions.terminate(ERROR, this);
					queue.clear();
					for (PublishInner<T> inner : terminate()) {
						inner.actual.onError(e);
					}
					return true;
				}
				else if (empty) {
					CONNECTION.compareAndSet(parent, this, null);
					for (PublishInner<T> inner : terminate()) {
						inner.actual.onComplete();
					}
					return true;
				}
			}
			return false;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case PARENT:
					return s;
				case PREFETCH:
					return prefetch;
				case ERROR:
					return error;
				case BUFFERED:
					return queue != null ? queue.size() : 0;
				case TERMINATED:
					return isTerminated();
				case CANCELLED:
					return cancelled;
			}
			return null;
		}

		@Override
		public boolean isDisposed() {
			return cancelled || done;
		}

	}

	static final class PublishInner<T> implements InnerProducer<T> {

		final Subscriber<? super T> actual;

		PublishSubscriber<T> parent;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishInner.class, "requested");

		PublishInner(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				requested(this, n);
				PublishSubscriber<T> p = parent;
				if (p != null) {
					p.drain();
				}
			}
		}

		static final long CANCEL_REQUEST = Long.MIN_VALUE;

		@Override
		public void cancel() {
			long r = requested;
			if (r != Long.MIN_VALUE) {
				r = REQUESTED.getAndSet(this, CANCEL_REQUEST);
				if (r != CANCEL_REQUEST) {
					PublishSubscriber<T> p = parent;
					if (p != null) {
						p.remove(this);
						p.drain();
					}
				}
			}
		}

		boolean isCancelled() {
			return requested == CANCEL_REQUEST;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return parent;
				case TERMINATED:
					return parent != null && parent.isTerminated();
				case CANCELLED:
					return isCancelled();
				case REQUESTED_FROM_DOWNSTREAM:
					return isCancelled() ? 0L : requested;
			}
			return InnerProducer.super.scan(key);
		}

		long produced(long n) {
			return produced(this, n);
		}

		//TODO factorize in Operators ?
		static void requested(PublishInner<?> inner, long n) {
			for (; ; ) {
				long r = REQUESTED.get(inner);
				if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
					return;
				}
				long u = Operators.addCap(r, n);
				if (REQUESTED.compareAndSet(inner, r, u)) {
					return;
				}
			}
		}

		static long produced(PublishInner<?> inner, long n) {
			for (; ; ) {
				long current = REQUESTED.get(inner);
				if (current == Long.MIN_VALUE) {
					return Long.MIN_VALUE;
				}
				if (current == Long.MAX_VALUE) {
					return Long.MAX_VALUE;
				}
				long update = current - n;
				if (update < 0L) {
					Operators.reportBadRequest(update);
					update = 0L;
				}
				if (REQUESTED.compareAndSet(inner, current, update)) {
					return update;
				}
			}
		}
	}
}
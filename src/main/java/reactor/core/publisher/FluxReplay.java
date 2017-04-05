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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.QueueSupplier;

/**
 * @param <T>
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxReplay<T> extends ConnectableFlux<T> implements Scannable, Fuseable {

	final Publisher<T>   source;
	final int            history;
	final long           ttl;
	final Scheduler scheduler;

	volatile ReplaySubscriber<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxReplay, ReplaySubscriber> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxReplay.class,
					ReplaySubscriber.class,
					"connection");

	FluxReplay(Publisher<T> source,
			int history,
			long ttl,
			Scheduler scheduler) {
		this.source = Objects.requireNonNull(source, "source");
		this.history = history;
		if(history < 0){
			throw new IllegalArgumentException("History cannot be negativ : "+history);
		}
		if (scheduler != null && ttl < 0) {
			throw new IllegalArgumentException("TTL cannot be negative : " + ttl);
		}
		this.ttl = ttl;
		this.scheduler = scheduler;
	}

	@Override
	public int getPrefetch() {
		return history;
	}

	ReplaySubscriber<T> newState() {
		if (scheduler != null) {
			return new ReplaySubscriber<>(new ReplayProcessor.SizeAndTimeBoundReplayBuffer<>(history,
					ttl,
					scheduler),
					this);
		}
		if (history != Integer.MAX_VALUE) {
			return new ReplaySubscriber<>(new ReplayProcessor.SizeBoundReplayBuffer<>(history),
					this);
		}
		return new ReplaySubscriber<>(new ReplayProcessor.UnboundedReplayBuffer<>(QueueSupplier.SMALL_BUFFER_SIZE),
					this);
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		boolean doConnect;
		ReplaySubscriber<T> s;
		for (; ; ) {
			s = connection;
			if (s == null || s.isTerminated()) {
				ReplaySubscriber<T> u = newState();
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
		ReplayInner<T> inner = new ReplayInner<>(s);
		s.onSubscribe(inner);
		for (; ; ) {
			if (inner.isCancelled()) {
				break;
			}

			ReplaySubscriber<T> c = connection;
			if (c == null || c.isTerminated()) {
				ReplaySubscriber<T> u = newState();
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
	public Object scan(Scannable.Attr key) {
		switch (key){
			case PREFETCH:
				return getPrefetch();
			case PARENT:
				return source;
		}
		return null;
	}

	static final class ReplaySubscriber<T>
			implements InnerConsumer<T>, Disposable {

		final FluxReplay<T>                   parent;
		final ReplayProcessor.ReplayBuffer<T> buffer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ReplaySubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ReplaySubscriber.class,
						Subscription.class,
						"s");

		volatile ReplayInner<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "wip");

		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "connected");

		@SuppressWarnings("rawtypes")
		static final ReplayInner[] EMPTY      = new ReplayInner[0];
		@SuppressWarnings("rawtypes")
		static final ReplayInner[] TERMINATED = new ReplayInner[0];

		volatile boolean cancelled;

		@SuppressWarnings("unchecked")
		ReplaySubscriber(ReplayProcessor.ReplayBuffer<T> buffer,
				FluxReplay<T> parent) {
			this.buffer = buffer;
			this.parent = parent;
			this.subscribers = EMPTY;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(buffer.isDone()){
				s.cancel();
			}
			else if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			ReplayProcessor.ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onNextDropped(t);
			}
			else {
				b.add(t);
				for (ReplayInner<T> rs : subscribers) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			ReplayProcessor.ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onErrorDropped(t);
			}
			else {
				b.onError(t);

				ReplayInner<T>[] a = subscribers;

				for (ReplayInner<T> rs : a) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onComplete() {
			ReplayProcessor.ReplayBuffer<T> b = buffer;
			if (!b.isDone()) {
				b.onComplete();

				ReplayInner<T>[] a = subscribers;

				for (ReplayProcessor.ReplaySubscription<T> rs : a) {
					b.replay(rs);
				}
			}
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
			CancellationException ex = new CancellationException("Disconnected");
			buffer.onError(ex);
			for (ReplayInner<T> inner : terminate()) {
				inner.actual.onError(ex);
			}
		}

		boolean add(ReplayInner<T> inner) {
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				ReplayInner<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;

				@SuppressWarnings("unchecked") ReplayInner<T>[] b =
						new ReplayInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;

				subscribers = b;
				return true;
			}
		}

		@SuppressWarnings("unchecked")
		void remove(ReplayInner<T> inner) {
			ReplayInner<T>[] a = subscribers;
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

				ReplayInner<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new ReplayInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}

				subscribers = b;
			}
		}

		@SuppressWarnings("unchecked")
		ReplayInner<T>[] terminate() {
			ReplayInner<T>[] a = subscribers;
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

		boolean isTerminated() {
			return subscribers == TERMINATED;
		}

		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}

		boolean trySubscribe(ReplayInner<T> inner) {
			if (add(inner)) {
				if (inner.isCancelled()) {
					remove(inner);
				}
				else {
					inner.parent = this;
					buffer.replay(inner);
				}
				return true;
			}
			return false;
		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case PARENT:
					return s;
				case PREFETCH:
					return Integer.MAX_VALUE;
				case CAPACITY:
					return buffer.capacity();
				case ERROR:
					return buffer.getError();
				case BUFFERED:
					return buffer.size();
				case TERMINATED:
					return isTerminated();
				case CANCELLED:
					return cancelled;
			}
			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public boolean isDisposed() {
			return cancelled || buffer.isDone();
		}

	}

	static final class ReplayInner<T>
			implements ReplayProcessor.ReplaySubscription<T> {

		final Subscriber<? super T> actual;

		ReplaySubscriber<T> parent;

		int index;

		int tailIndex;

		Object node;

		int fusionMode;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplayInner> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplayInner.class, "wip");


		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReplayInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ReplayInner.class, "requested");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplayInner> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(ReplayInner.class,
						"cancelled");

		ReplayInner(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (fusionMode() == NONE) {
					Operators.getAndAddCap(REQUESTED, this, n);
				}
				ReplaySubscriber<T> p = parent;
				if (p != null) {
					p.buffer.replay(this);
				}
			}
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return parent;
				case TERMINATED:
					return parent != null && parent.isTerminated();
				case BUFFERED:
					return size();
				case CANCELLED:
					return isCancelled();
				case REQUESTED_FROM_DOWNSTREAM:
					return isCancelled() ? 0L : requested;
			}
			return ReplayProcessor.ReplaySubscription.super.scan(key);
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				ReplaySubscriber<T> p = parent;
				if (p != null) {
					p.remove(this);
				}
				if (enter()) {
					node = null;
				}
			}
		}

		@Override
		public long requested() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled == 1;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				fusionMode = ASYNC;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public T poll() {
			ReplaySubscriber<T> p = parent;
			if(p != null){
				return p.buffer.poll(this);
			}
			return null;
		}

		@Override
		public void clear() {
			ReplaySubscriber<T> p = parent;
			if(p != null) {
				p.buffer.clear(this);
			}
		}

		@Override
		public boolean isEmpty() {
			ReplaySubscriber<T> p = parent;
			return p == null || p.buffer.isEmpty(this);
		}

		@Override
		public int size() {
			ReplaySubscriber<T> p = parent;
			if(p != null) {
				return p.buffer.size(this);
			}
			return 0;
		}

		@Override
		public void node(Object node) {
			this.node = node;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}



		@Override
		public Object node() {
			return node;
		}

		@Override
		public int index() {
			return index;
		}

		@Override
		public void index(int index) {
			this.index = index;
		}

		@Override
		public int tailIndex() {
			return tailIndex;
		}

		@Override
		public void tailIndex(int tailIndex) {
			this.tailIndex = tailIndex;
		}

		@Override
		public boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}

		@Override
		public int leave(int missed) {
			return WIP.addAndGet(this, -missed);
		}

		@Override
		public void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}
}

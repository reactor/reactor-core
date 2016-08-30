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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Fuseable;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.concurrent.QueueSupplier;

/**
 * @param <T>
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxReplay<T> extends ConnectableFlux<T>
		implements Producer, Fuseable {

	final Publisher<T>   source;
	final int            history;
	final long           ttl;
	final TimedScheduler scheduler;

	volatile State<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxReplay, State> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxReplay.class,
					State.class,
					"connection");

	FluxReplay(Publisher<T> source,
			int history,
			long ttl,
			TimedScheduler scheduler) {
		this.source = Objects.requireNonNull(source, "source");
		this.history = history;
		if (scheduler != null && ttl < 0) {
			throw new IllegalArgumentException("TTL cannot be negative : " + ttl);
		}
		this.ttl = ttl;
		this.scheduler = scheduler;
	}

	@Override
	public Object downstream() {
		return connection;
	}

	State<T> newState() {
		if (scheduler != null) {
			return new State<>(new ReplayProcessor.SizeAndTimeBoundReplayBuffer<>(history,
					ttl,
					scheduler),
					this);
		}
		if (history != Integer.MAX_VALUE) {
			return new State<>(new ReplayProcessor.SizeBoundReplayBuffer<>(history),
					this);
		}
		return new State<>(new ReplayProcessor.UnboundedReplayBuffer<>(QueueSupplier.SMALL_BUFFER_SIZE),
					this);
	}

	@Override
	public void connect(Consumer<? super Cancellation> cancelSupport) {
		boolean doConnect;
		State<T> s;
		for (; ; ) {
			s = connection;
			if (s == null || s.isTerminated()) {
				State<T> u = newState();
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
		InnerSubscription<T> inner = new InnerSubscription<>(s);
		s.onSubscribe(inner);
		for (; ; ) {
			if (inner.isCancelled()) {
				break;
			}

			State<T> c = connection;
			if (c == null || c.isTerminated()) {
				State<T> u = newState();
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
	public Object upstream() {
		return source;
	}

	static final class State<T>
			implements Subscriber<T>, Receiver, MultiProducer, Trackable, Cancellation {

		final FluxReplay<T>                   parent;
		final ReplayProcessor.ReplayBuffer<T> buffer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<State, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(State.class,
						Subscription.class,
						"s");

		volatile InnerSubscription<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> WIP =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "wip");

		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "connected");

		@SuppressWarnings("rawtypes")
		static final InnerSubscription[] EMPTY      = new InnerSubscription[0];
		@SuppressWarnings("rawtypes")
		static final InnerSubscription[] TERMINATED = new InnerSubscription[0];

		volatile boolean cancelled;

		@SuppressWarnings("unchecked")
		public State(ReplayProcessor.ReplayBuffer<T> buffer,
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
				for (InnerSubscription<T> rs : subscribers) {
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

				InnerSubscription<T>[] a = subscribers;

				for (InnerSubscription<T> rs : a) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onComplete() {
			ReplayProcessor.ReplayBuffer<T> b = buffer;
			if (!b.isDone()) {
				b.onComplete();

				InnerSubscription<T>[] a = subscribers;

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
			for (InnerSubscription<T> inner : terminate()) {
				inner.actual.onError(ex);
			}
		}

		boolean add(InnerSubscription<T> inner) {
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				InnerSubscription<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;

				@SuppressWarnings("unchecked") InnerSubscription<T>[] b =
						new InnerSubscription[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;

				subscribers = b;
				return true;
			}
		}

		@SuppressWarnings("unchecked")
		void remove(InnerSubscription<T> inner) {
			InnerSubscription<T>[] a = subscribers;
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

				InnerSubscription<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new InnerSubscription[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}

				subscribers = b;
			}
		}

		@SuppressWarnings("unchecked")
		InnerSubscription<T>[] terminate() {
			InnerSubscription<T>[] a = subscribers;
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

		@Override
		public boolean isTerminated() {
			return subscribers == TERMINATED;
		}

		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}

		boolean trySubscribe(InnerSubscription<T> inner) {
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
		public long getCapacity() {
			return buffer instanceof ReplayProcessor.UnboundedReplayBuffer ?
					Long.MAX_VALUE : buffer.capacity();
		}

		@Override
		public long getPending() {
			return buffer.size();
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return !cancelled && !buffer.isDone() && s != null;
		}

		@Override
		public Throwable getError() {
			return buffer.getError();
		}

		@Override
		public Iterator<?> downstreams() {
			return Arrays.asList(subscribers)
			             .iterator();
		}

		@Override
		public long downstreamCount() {
			return subscribers.length;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}

	static final class InnerSubscription<T>
			implements ReplayProcessor.ReplaySubscription<T>, Receiver {

		final Subscriber<? super T> actual;

		State<T> parent;

		int index;

		int tailIndex;

		Object node;

		int fusionMode;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<InnerSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(InnerSubscription.class, "wip");


		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerSubscription.class, "requested");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<InnerSubscription> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(InnerSubscription.class,
						"cancelled");

		public InnerSubscription(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (fusionMode() == NONE) {
					Operators.getAndAddCap(REQUESTED, this, n);
				}
				State<T> p = parent;
				if (p != null) {
					p.buffer.replay(this);
				}
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				State<T> p = parent;
				if (p != null) {
					p.remove(this);
				}
				if (enter()) {
					node = null;
				}
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled == 1;
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return parent;
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
			State<T> p = parent;
			if(p != null){
				return p.buffer.poll(this);
			}
			return null;
		}

		@Override
		public void clear() {
			State<T> p = parent;
			if(p != null) {
				p.buffer.clear(this);
			}
		}

		@Override
		public boolean isEmpty() {
			State<T> p = parent;
			if(p != null) {
				p.buffer.isEmpty(this);
			}
			return true;
		}

		@Override
		public int size() {
			State<T> p = parent;
			if(p != null) {
				p.buffer.size(this);
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

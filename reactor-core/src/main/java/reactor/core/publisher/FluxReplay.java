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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

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

	interface ReplaySubscription<T> extends QueueSubscription<T>, InnerProducer<T> {

		CoreSubscriber<? super T> actual();

		boolean enter();

		int leave(int missed);

		void produced(long n);

		void node(@Nullable Object node);

		@Nullable
		Object node();

		int tailIndex();

		void tailIndex(int tailIndex);

		int index();

		void index(int index);

		int fusionMode();

		boolean isCancelled();

		long requested();
	}

	interface ReplayBuffer<T> {

		void add(T value);

		void onError(Throwable ex);

		@Nullable
		Throwable getError();

		void onComplete();

		void replay(ReplaySubscription<T> rs);

		boolean isDone();

		@Nullable
		T poll(ReplaySubscription<T> rs);

		void clear(ReplaySubscription<T> rs);

		boolean isEmpty(ReplaySubscription<T> rs);

		int size(ReplaySubscription<T> rs);

		int size();

		int capacity();

		boolean isExpired();
	}

	static final class SizeAndTimeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		static final class TimedNode<T> extends AtomicReference<TimedNode<T>> {

			final T    value;
			final long time;

			TimedNode(@Nullable T value, long time) {
				this.value = value;
				this.time = time;
			}
		}

		final int            limit;
		final long           maxAge;
		final Scheduler scheduler;
		int size;

		volatile TimedNode<T> head;

		TimedNode<T> tail;

		Throwable error;
		static final long NOT_DONE = Long.MIN_VALUE;

		volatile long done = NOT_DONE;

		SizeAndTimeBoundReplayBuffer(int limit,
				long maxAge,
				Scheduler scheduler) {
			this.limit = limit;
			this.maxAge = maxAge;
			this.scheduler = scheduler;
			TimedNode<T> h = new TimedNode<>(null, 0L);
			this.tail = h;
			this.head = h;
		}

		@Override
		public boolean isExpired() {
			long done = this.done;
			return done != NOT_DONE && scheduler.now(TimeUnit.MILLISECONDS) - maxAge > done;
		}

		@SuppressWarnings("unchecked")
		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;
			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {
				@SuppressWarnings("unchecked") TimedNode<T> node =
						(TimedNode<T>) rs.node();
				if (node == null) {
					node = head;
					if (done == NOT_DONE) {
						// skip old entries
						long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
						TimedNode<T> next = node;
						while (next != null) {
							long ts = next.time;
							if (ts > limit) {
								break;
							}
							node = next;
							next = node.get();
						}
					}
				}

				long r = rs.requested();
				long e = 0L;

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done != NOT_DONE;
					TimedNode<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(next.value);

					e++;
					node = next;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done != NOT_DONE;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done != NOT_DONE;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void onError(Throwable ex) {
			done = scheduler.now(TimeUnit.MILLISECONDS);
			error = ex;
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		@Override
		public void onComplete() {
			done = scheduler.now(TimeUnit.MILLISECONDS);
		}

		@Override
		public boolean isDone() {
			return done != NOT_DONE;
		}

		@SuppressWarnings("unchecked")
		TimedNode<T> latestHead(ReplaySubscription<T> rs) {
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			TimedNode<T> h = (TimedNode<T>) rs.node();
			if (h == null) {
				h = head;
			}
			TimedNode<T> n;
			while ((n = h.get()) != null) {
				if (n.time > now) {
					break;
				}
				h = n;
			}
			return h;
		}

		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			TimedNode<T> next;
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
			while ((next = node.get()) != null) {
				if (next.time > now) {
					node = next;
					break;
				}
				node = next;
			}
			if (next == null) {
				return null;
			}
			rs.node(next);

			return node.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isEmpty(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int size() {
			TimedNode<T> node = head;
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int capacity() {
			return limit;
		}

		@Override
		public void add(T value) {
			TimedNode<T> n = new TimedNode<>(value, scheduler.now(TimeUnit.MILLISECONDS));
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
			long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			TimedNode<T> h = head;
			TimedNode<T> next;
			int removed = 0;
			for (; ; ) {
				next = h.get();
				if (next == null) {
					break;
				}

				if (next.time > limit) {
					if (removed != 0) {
						size = size - removed;
						head = h;
					}
					break;
				}

				h = next;
				removed++;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}
	}

	static final class UnboundedReplayBuffer<T> implements ReplayBuffer<T> {

		final int batchSize;

		volatile int size;

		final Object[] head;

		Object[] tail;

		int tailIndex;

		volatile boolean done;
		Throwable error;

		UnboundedReplayBuffer(int batchSize) {
			this.batchSize = batchSize;
			Object[] n = new Object[batchSize + 1];
			this.tail = n;
			this.head = n;
		}

		@Override
		public boolean isExpired() {
			return false;
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		@Override
		public int capacity() {
			return Integer.MAX_VALUE;
		}

		@Override
		public void add(T value) {
			int i = tailIndex;
			Object[] a = tail;
			if (i == a.length - 1) {
				Object[] b = new Object[a.length];
				b[0] = value;
				tailIndex = 1;
				a[i] = b;
				tail = b;
			}
			else {
				a[i] = value;
				tailIndex = i + 1;
			}
			size++;
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();
			final int n = batchSize;

			for (; ; ) {

				long r = rs.requested();
				long e = 0L;

				Object[] node = (Object[]) rs.node();
				if (node == null) {
					node = head;
				}
				int tailIndex = rs.tailIndex();
				int index = rs.index();

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					if (tailIndex == n) {
						node = (Object[]) node[tailIndex];
						tailIndex = 0;
					}

					@SuppressWarnings("unchecked") T v = (T) node[tailIndex];

					a.onNext(v);

					e++;
					tailIndex++;
					index++;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.index(index);
				rs.tailIndex(tailIndex);
				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			int index = rs.index();
			if (index == size) {
				return null;
			}
			Object[] node = (Object[]) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			int tailIndex = rs.tailIndex();
			if (tailIndex == batchSize) {
				node = (Object[]) node[tailIndex];
				tailIndex = 0;
				rs.node(node);
			}
			@SuppressWarnings("unchecked") T v = (T) node[tailIndex];
			rs.index(index + 1);
			rs.tailIndex(tailIndex + 1);
			return v;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			return rs.index() == size;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			return size - rs.index();
		}

		@Override
		public int size() {
			return size;
		}

	}

	static final class SizeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		final int limit;

		volatile Node<T> head;

		Node<T> tail;

		int size;

		volatile boolean done;
		Throwable error;

		SizeBoundReplayBuffer(int limit) {
			if(limit < 0){
				throw new IllegalArgumentException("Limit cannot be negative");
			}
			this.limit = limit;
			Node<T> n = new Node<>(null);
			this.tail = n;
			this.head = n;
		}

		@Override
		public boolean isExpired() {
			return false;
		}

		@Override
		public int capacity() {
			return limit;
		}

		@Override
		public void add(T value) {
			Node<T> n = new Node<>(value);
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		void replayNormal(ReplaySubscription<T> rs) {
			final Subscriber<? super T> a = rs.actual();

			int missed = 1;

			for (; ; ) {

				long r = rs.requested();
				long e = 0L;

				@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
				if (node == null) {
					node = head;
				}

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					Node<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(next.value);

					e++;
					node = next;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		static final class Node<T> extends AtomicReference<Node<T>> {

			/** */
			private static final long serialVersionUID = 3713592843205853725L;

			final T value;

			Node(@Nullable T value) {
				this.value = value;
			}
		}

		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}

			Node<T> next = node.get();
			if (next == null) {
				return null;
			}
			rs.node(next);

			return next.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
			}
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int size() {
			Node<T> node = head;
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}
	}

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxReplay, ReplaySubscriber> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxReplay.class,
					ReplaySubscriber.class,
					"connection");

	FluxReplay(Publisher<T> source,
			int history,
			long ttl,
			@Nullable Scheduler scheduler) {
		this.source = Objects.requireNonNull(source, "source");
		this.history = history;
		if(history < 0){
			throw new IllegalArgumentException("History cannot be negative : " + history);
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
			return new ReplaySubscriber<>(new SizeAndTimeBoundReplayBuffer<>(history,
					ttl,
					scheduler),
					this);
		}
		if (history != Integer.MAX_VALUE) {
			return new ReplaySubscriber<>(new SizeBoundReplayBuffer<>(history),
					this);
		}
		return new ReplaySubscriber<>(new UnboundedReplayBuffer<>(Queues.SMALL_BUFFER_SIZE),
					this);
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		boolean doConnect;
		ReplaySubscriber<T> s;
		for (; ; ) {
			s = connection;
			if (s == null) {
				ReplaySubscriber<T> u = newState();
				if (!CONNECTION.compareAndSet(this, null, u)) {
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
	public void subscribe(CoreSubscriber<? super T> actual) {
		ReplayInner<T> inner = new ReplayInner<>(actual);
		for (; ; ) {
			ReplaySubscriber<T> c = connection;
			if (scheduler != null && c != null && c.buffer.isExpired()) {
				ReplaySubscriber<T> u = newState();
				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}
				c = u;
				source.subscribe(u);
			}
			else if (c == null) {
				ReplaySubscriber<T> u = newState();
				if (!CONNECTION.compareAndSet(this, null, u)) {
					continue;
				}

				c = u;
			}

			c.add(inner);

			actual.onSubscribe(inner);

			if (inner.isCancelled()) {
				c.remove(inner);
			}

			inner.parent = c;
			c.buffer.replay(inner);

			break;
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;

		return null;
	}

	static final class ReplaySubscriber<T>
			implements InnerConsumer<T>, Disposable {

		final FluxReplay<T>   parent;
		final ReplayBuffer<T> buffer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ReplaySubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ReplaySubscriber.class,
						Subscription.class,
						"s");

		volatile ReplaySubscription<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "wip");

		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "connected");

		@SuppressWarnings("rawtypes")
		static final ReplaySubscription[] EMPTY      = new ReplaySubscription[0];
		@SuppressWarnings("rawtypes")
		static final ReplaySubscription[] TERMINATED = new ReplaySubscription[0];

		volatile boolean cancelled;

		@SuppressWarnings("unchecked")
		ReplaySubscriber(ReplayBuffer<T> buffer,
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
			ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onNextDropped(t, currentContext());
			}
			else {
				b.add(t);
				for (ReplaySubscription<T> rs : subscribers) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onErrorDropped(t, currentContext());
			}
			else {
				b.onError(t);

				for (ReplaySubscription<T> rs : terminate()) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onComplete() {
			ReplayBuffer<T> b = buffer;
			if (!b.isDone()) {
				b.onComplete();

				for (ReplaySubscription<T> rs : terminate()) {
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

				CONNECTION.lazySet(parent, null);

				CancellationException ex = new CancellationException("Disconnected");
				buffer.onError(ex);

				for (ReplaySubscription<T> inner : terminate()) {
					buffer.replay(inner);
				}

			}
		}

		boolean add(ReplayInner<T> inner) {
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				ReplaySubscription<T>[] a = subscribers;
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
		void remove(ReplaySubscription<T> inner) {
			ReplaySubscription<T>[] a = subscribers;
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

				ReplaySubscription<T>[] b;
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
		ReplaySubscription<T>[] terminate() {
			ReplaySubscription<T>[] a = subscribers;
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

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.CAPACITY) return buffer.capacity();
			if (key == Attr.ERROR) return buffer.getError();
			if (key == Attr.BUFFERED) return buffer.size();
			if (key == Attr.TERMINATED) return isTerminated();
			if (key == Attr.CANCELLED) return cancelled;

			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public boolean isDisposed() {
			return cancelled;
		}

	}

	static final class ReplayInner<T>
			implements ReplaySubscription<T> {

		final CoreSubscriber<? super T> actual;

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

		ReplayInner(CoreSubscriber<? super T> actual) {
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED) return parent != null && parent.isTerminated();
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return isCancelled() ? 0L : requested;

			return ReplaySubscription.super.scanUnsafe(key);
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
		public CoreSubscriber<? super T> actual() {
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
		@Nullable
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
		public void node(@Nullable Object node) {
			this.node = node;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		@Nullable
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

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 * @author David Karnok
 */
//adapted from RxJava2Extensions: https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableRefCountTimeout.java
final class FluxRefCountGrace<T> extends Flux<T> implements Scannable, Fuseable {

	final ConnectableFlux<T> source;
	final int                n;
	final Duration           gracePeriod;
	final Scheduler          scheduler;

	RefConnection connection;

	FluxRefCountGrace(ConnectableFlux<T> source, int n, Duration gracePeriod, Scheduler scheduler) {
		this.source = source;
		this.n = n;
		this.gracePeriod = gracePeriod;
		this.scheduler = scheduler;
	}


	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		RefConnection conn;

		boolean connect = false;
		synchronized (this) {
			conn = connection;
			if (conn == null || conn.terminated) {
				conn = new RefConnection(this);
				connection = conn;
			}

			long c = conn.subscriberCount;
			if (c == 0L && conn.timer != null) {
				conn.timer.dispose();
			}
			conn.subscriberCount = c + 1;
			if (!conn.connected && c + 1 == n) {
				connect = true;
				conn.connected = true;
			}
		}

		source.subscribe(new RefCountInner<>(actual, this, conn));

		if (connect) {
			source.connect(conn);
		}
	}

	void cancel(RefConnection rc) {
		boolean replaceTimer = false;
		Disposable dispose = null;
		Disposable.Swap sd = null;
		synchronized (this) {
			if (rc.terminated) {
				return;
			}
			long c = rc.subscriberCount - 1;
			rc.subscriberCount = c;
			if (c != 0L || !rc.connected) {
				return;
			}
			if (!gracePeriod.isZero()) {
				sd = Disposables.swap();
				rc.timer = sd;
				replaceTimer = true;
			}
			else if (rc == connection) {
				//emulate what a timeout would do without getting out of sync block
				//capture the disposable for later disposal
				connection = null;
				dispose = RefConnection.SOURCE_DISCONNECTOR.getAndSet(rc, Disposables.disposed());
			}
		}

		if (replaceTimer) {
			sd.replace(scheduler.schedule(rc, gracePeriod.toNanos(), TimeUnit.NANOSECONDS));
		} else if (dispose != null) {
			dispose.dispose();
		}
	}

	void terminated(RefConnection rc) {
		synchronized (this) {
			if (!rc.terminated) {
				rc.terminated = true;
				connection = null;
			}
		}
	}

	void timeout(RefConnection rc) {
		Disposable dispose = null;
		synchronized (this) {
			if (rc.subscriberCount == 0 && rc == connection) {
				connection = null;
				dispose = RefConnection.SOURCE_DISCONNECTOR.getAndSet(rc, Disposables.disposed());
			}
		}
		if (dispose != null) {
			dispose.dispose();
		}
	}

	static final class RefConnection implements Runnable, Consumer<Disposable> {

		final FluxRefCountGrace<?> parent;

		Disposable timer;
		long       subscriberCount;
		boolean    connected;
		boolean    terminated;

		volatile Disposable sourceDisconnector;
		static final AtomicReferenceFieldUpdater<RefConnection, Disposable> SOURCE_DISCONNECTOR =
				AtomicReferenceFieldUpdater.newUpdater(RefConnection.class, Disposable.class, "sourceDisconnector");

		RefConnection(FluxRefCountGrace<?> parent) {
			this.parent = parent;
		}

		@Override
		public void run() {
			parent.timeout(this);
		}

		@Override
		public void accept(Disposable t) {
			OperatorDisposables.replace(SOURCE_DISCONNECTOR, this, t);
		}
	}

	static final class RefCountInner<T> implements QueueSubscription<T>, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final FluxRefCountGrace<T> parent;

		final RefConnection connection;

		Subscription s;
		QueueSubscription<T> qs;

		volatile int parentDone;
		static final AtomicIntegerFieldUpdater<RefCountInner> PARENT_DONE =
				AtomicIntegerFieldUpdater.newUpdater(RefCountInner.class, "parentDone");

		RefCountInner(CoreSubscriber<? super T> actual, FluxRefCountGrace<T> parent,
				RefConnection connection) {
			this.actual = actual;
			this.parent = parent;
			this.connection = connection;
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (PARENT_DONE.compareAndSet(this, 0, 1)) {
				parent.terminated(connection);
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (PARENT_DONE.compareAndSet(this, 0, 1)) {
				parent.terminated(connection);
			}
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			if (PARENT_DONE.compareAndSet(this, 0, 1)) {
				parent.cancel(connection);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public int requestFusion(int requestedMode) {
			if(s instanceof QueueSubscription){
				qs = (QueueSubscription<T>)s;
				return qs.requestFusion(requestedMode);
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			return qs.poll();
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		public boolean isEmpty() {
			return qs.isEmpty();
		}

		@Override
		public void clear() {
			qs.clear();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return InnerOperator.super.scanUnsafe(key);
		}
	}

}

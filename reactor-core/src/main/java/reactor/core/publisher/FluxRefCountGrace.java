/*
 * Copyright (c) 2017-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
		RefCountInner<T> inner = new RefCountInner<>(actual, this);

		source.subscribe(inner);

		boolean connect = false;
		synchronized (this) {
			conn = connection;
			if (conn == null || conn.isTerminated()) {
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

		inner.setRefConnection(conn);

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

		/**
		 * Indicates whether the RefConnection is terminated OR the source's connection has been disposed
		 * @return true if the connection can be considered as terminated, either by this operator or by the source
		 */
		boolean isTerminated() {
			Disposable sd = sourceDisconnector;
			return terminated || (sd != null && sd.isDisposed());
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

		RefConnection connection;

		Subscription s;
		QueueSubscription<T> qs;

		Throwable error;


		static final int MONITOR_SET_FLAG = 0b0010_0000_0000_0000_0000_0000_0000_0000;
		static final int TERMINATED_FLAG  = 0b0100_0000_0000_0000_0000_0000_0000_0000;
		static final int CANCELLED_FLAG   = 0b1000_0000_0000_0000_0000_0000_0000_0000;

		volatile int state; //used to guard against doubly terminating subscribers (e.g. double cancel)
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RefCountInner> STATE =
				AtomicIntegerFieldUpdater.newUpdater(RefCountInner.class, "state");

		RefCountInner(CoreSubscriber<? super T> actual, FluxRefCountGrace<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		void setRefConnection(RefConnection connection) {
			this.connection = connection;
			this.actual.onSubscribe(this);

			for (;;) {
				int previousState = this.state;

				if (isCancelled(previousState)) {
					return;
				}

				if (isTerminated(previousState)) {
					this.parent.terminated(connection);
					Throwable e = this.error;
					if (e != null) {
						this.actual.onError(e);
					}
					else {
						this.actual.onComplete();
					}
					return;
				}

				if (STATE.compareAndSet(this, previousState, previousState | MONITOR_SET_FLAG)) {
					return;
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			this.error = t;
			for (;;) {
				int previousState = this.state;

				if (isTerminated(previousState) || isCancelled(previousState)) {
					Operators.onErrorDropped(t, actual.currentContext());
					return;
				}

				if (STATE.compareAndSet(this, previousState, previousState | TERMINATED_FLAG)) {
					if (isMonitorSet(previousState)) {
						this.parent.terminated(connection);
						this.actual.onError(t);
					}
					return;
				}
			}
		}

		@Override
		public void onComplete() {
			for (;;) {
				int previousState = this.state;

				if (isTerminated(previousState) || isCancelled(previousState)) {
					return;
				}

				if (STATE.compareAndSet(this, previousState, previousState | TERMINATED_FLAG)) {
					if (isMonitorSet(previousState)) {
						this.parent.terminated(connection);
						this.actual.onComplete();
					}
					return;
				}
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();

			int previousState = this.state;

			if (isTerminated(previousState) || isCancelled(previousState)) {
				return;
			}

			if (STATE.compareAndSet(this, previousState, previousState | CANCELLED_FLAG)) {
				parent.cancel(connection);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
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
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return isTerminated(state);
			if (key == Attr.CANCELLED) return isCancelled(state);

			return InnerOperator.super.scanUnsafe(key);
		}

		static boolean isTerminated(int state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}

		static boolean isCancelled(int state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static boolean isMonitorSet(int state) {
			return (state & MONITOR_SET_FLAG) == MONITOR_SET_FLAG;
		}
	}

}

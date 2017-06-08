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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Connects to the underlying Flux once the given number of Subscribers subscribed
 * to it and disconnects once all Subscribers cancelled their Subscriptions.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRefCount<T> extends Flux<T> implements Scannable, Fuseable {

	final ConnectableFlux<? extends T> source;
	
	final int n;

	volatile RefCountMonitor<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxRefCount, RefCountMonitor> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxRefCount.class, RefCountMonitor.class, "connection");

	FluxRefCount(ConnectableFlux<? extends T> source, int n) {
		if (n <= 0) {
			throw new IllegalArgumentException("n > 0 required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.n = n;
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s, Context context) {
		RefCountMonitor<T> state;
		
		for (;;) {
			state = connection;
			if (state == null || Disposables.isDisposed(state.disconnect)) {
				RefCountMonitor<T> u = new RefCountMonitor<>(n, this);
				
				if (!CONNECTION.compareAndSet(this, state, u)) {
					continue;
				}
				
				state = u;
			}
			
			state.subscribe(s, context);
			break;
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == IntAttr.PREFETCH) return getPrefetch();
		if (key == ScannableAttr.PARENT) return source;

		return null;
	}

	static final class RefCountMonitor<T> implements Consumer<Disposable> {
		
		final int n;
		
		final FluxRefCount<? extends T> parent;
		
		volatile int subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RefCountMonitor> SUBSCRIBERS =
				AtomicIntegerFieldUpdater.newUpdater(RefCountMonitor.class, "subscribers");
		
		volatile Disposable disconnect;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<RefCountMonitor, Disposable> DISCONNECT =
				AtomicReferenceFieldUpdater.newUpdater(RefCountMonitor.class, Disposable.class, "disconnect");
		
		RefCountMonitor(int n, FluxRefCount<? extends T> parent) {
			this.n = n;
			this.parent = parent;
		}
		
		void subscribe(Subscriber<? super T> s, Context context) {
			// FIXME think about what happens when subscribers come and go below the connection threshold concurrently

			RefCountInner<T> inner = new RefCountInner<>(s, this);
			parent.source.subscribe(inner, context);
			
			if (SUBSCRIBERS.incrementAndGet(this) == n) {
				parent.source.connect(this);
			}
		}
		
		@Override
		public void accept(Disposable r) {
			if (!DISCONNECT.compareAndSet(this, null, r)) {
				r.dispose();
			}
		}

		void innerCancelled() {
			if (SUBSCRIBERS.decrementAndGet(this) == 0) {
				Disposables.dispose(DISCONNECT, this);
			}
		}
		
		void upstreamFinished() {
			Disposable a = disconnect;
			if (a != Disposables.DISPOSED) {
				DISCONNECT.getAndSet(this, Disposables.DISPOSED);
			}
		}
	}

	static final class RefCountInner<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		final RefCountMonitor<T> parent;

		Subscription s;
		QueueSubscription<T> qs;

		RefCountInner(Subscriber<? super T> actual, RefCountMonitor<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if(key== ScannableAttr. PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
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
			actual.onError(t);
			parent.upstreamFinished();
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			parent.upstreamFinished();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			parent.innerCancelled();
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
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
	}
}

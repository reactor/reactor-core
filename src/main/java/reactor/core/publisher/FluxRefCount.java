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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.util.BackpressureUtils;

/**
 * Connects to the underlying Stream once the given number of Subscribers subscribed
 * to it and disconnects once all Subscribers cancelled their Subscriptions.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxRefCount<T> extends Flux<T>
		implements Receiver, Loopback {
	
	final ConnectableFlux<? extends T> source;
	
	final int n;

	volatile State<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxRefCount, State> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxRefCount.class, State.class, "connection");
	
	public FluxRefCount(ConnectableFlux<? extends T> source, int n) {
		if (n <= 0) {
			throw new IllegalArgumentException("n > 0 required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.n = n;
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		State<T> state;
		
		for (;;) {
			state = connection;
			if (state == null || state.isDisconnected()) {
				State<T> u = new State<>(n, this);
				
				if (!CONNECTION.compareAndSet(this, state, u)) {
					continue;
				}
				
				state = u;
			}
			
			state.subscribe(s);
			break;
		}
	}

	@Override
	public Object connectedOutput() {
		return connection;
	}

	@Override
	public Object upstream() {
		return source;
	}

	static final class State<T> implements Consumer<Runnable>, MultiProducer, Receiver {
		
		final int n;
		
		final FluxRefCount<? extends T> parent;
		
		volatile int subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> SUBSCRIBERS =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "subscribers");
		
		volatile Runnable disconnect;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<State, Runnable> DISCONNECT =
				AtomicReferenceFieldUpdater.newUpdater(State.class, Runnable.class, "disconnect");
		
		static final Runnable DISCONNECTED = new Runnable() {
			@Override
			public void run() { 
				
			}
		};

		public State(int n, FluxRefCount<? extends T> parent) {
			this.n = n;
			this.parent = parent;
		}
		
		void subscribe(Subscriber<? super T> s) {
			// FIXME think about what happens when subscribers come and go below the connection threshold concurrently
			
			InnerSubscriber<T> inner = new InnerSubscriber<>(s, this);
			parent.source.subscribe(inner);
			
			if (SUBSCRIBERS.incrementAndGet(this) == n) {
				parent.source.connect(this);
			}
		}
		
		@Override
		public void accept(Runnable r) {
			if (!DISCONNECT.compareAndSet(this, null, r)) {
				r.run();
			}
		}
		
		void doDisconnect() {
			Runnable a = disconnect;
			if (a != DISCONNECTED) {
				a = DISCONNECT.getAndSet(this, DISCONNECTED);
				if (a != null && a != DISCONNECTED) {
					a.run();
				}
			}
		}
		
		boolean isDisconnected() {
			return disconnect == DISCONNECTED;
		}
		
		void innerCancelled() {
			if (SUBSCRIBERS.decrementAndGet(this) == 0) {
				doDisconnect();
			}
		}
		
		void upstreamFinished() {
			Runnable a = disconnect;
			if (a != DISCONNECTED) {
				DISCONNECT.getAndSet(this, DISCONNECTED);
			}
		}

		@Override
		public Iterator<?> downstreams() {
			return null;
		}

		@Override
		public long downstreamCount() {
			return subscribers;
		}

		@Override
		public Object upstream() {
			return parent;
		}

		static final class InnerSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer, Loopback {

			final Subscriber<? super T> actual;
			
			final State<T> parent;
			
			Subscription s;
			
			public InnerSubscriber(Subscriber<? super T> actual, State<T> parent) {
				this.actual = actual;
				this.parent = parent;
			}

			@Override
			public void onSubscribe(Subscription s) {
				if (BackpressureUtils.validate(this.s, s)) {
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
			public Object downstream() {
				return actual;
			}

			@Override
			public Object upstream() {
				return s;
			}

			@Override
			public Object connectedInput() {
				return null;
			}

			@Override
			public Object connectedOutput() {
				return s;
			}
		}
	}
}

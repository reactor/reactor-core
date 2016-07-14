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

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.subscriber.SubscriberState;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.Exceptions;

/**
 * @param <T>
 * @param <U>
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class ConnectableFluxProcess<T, U> extends ConnectableFlux<U> implements Producer {

	final Publisher<T>												 source;
	final Supplier<? extends Processor<? super T, ? extends T>>		processorSupplier;
	final Function<Flux<T>, ? extends Publisher<? extends U>> selector;

	volatile State<T, U> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ConnectableFluxProcess, State> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(ConnectableFluxProcess.class, State.class, "connection");

	ConnectableFluxProcess(Publisher<T> source,
			Supplier<? extends Processor<? super T, ? extends T>> processorSupplier,
			Function<Flux<T>, ? extends Publisher<? extends U>> selector) {
		this.source = Objects.requireNonNull(source, "source");
		this.processorSupplier = Objects.requireNonNull(processorSupplier, "processorSupplier");
		this.selector = Objects.requireNonNull(selector, "selector");
	}

	@Override
	public Object downstream() {
		return connection;
	}

	@Override
	public void connect(Consumer<? super Cancellation> cancelSupport) {
		boolean doConnect;
		State<T, U> s;

		for (; ; ) {
			s = connection;
			if (s == null || s.isTerminated()) {
				Processor<? super T, ? extends T> p = processorSupplier.get();
				State<T, U> u;
				if(p instanceof Fuseable && source instanceof Fuseable){
					u = new StateFuseable<>(p, selector.apply(from(p)));
				}
				else{
					u = new StateNormal<>(p, selector.apply(from(p)));
				}

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
	public void subscribe(Subscriber<? super U> s) {
		for (; ; ) {
			State<T, U> c = connection;
			if (c == null || c.isTerminated()) {
				Processor<? super T, ? extends T> p = processorSupplier.get();
				State<T, U> u;
				if(p instanceof Fuseable && source instanceof Fuseable){
					u = new StateFuseable<>(p, selector.apply(from(p)));
				}
				else{
					u = new StateNormal<>(p, selector.apply(from(p)));
				}

				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}

				c = u;
			}

			c.publisher.subscribe(s);
			break;
		}

	}

	@Override
	public Object upstream() {
		return source;
	}

	static abstract class State<T, U>
			implements Cancellation, Subscription, Receiver, Producer, Subscriber<T>,
			           SubscriberState {

		final Processor<? super T, ? extends T> processor;
		final Publisher<? extends U>			publisher;


		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "connected");

		public State(Processor<? super T, ? extends T> processor, Publisher<? extends U> publisher) {
			this.processor = processor;
			this.publisher = publisher;
		}

		@Override
		public boolean isStarted() {
			return connected == 1;
		}

		@Override
		public boolean isTerminated() {
			return connected == 2;
		}

		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}

		@Override
		public void onNext(T t) {
			if(isTerminated()){
				Exceptions.onNextDropped(t);
				return;
			}
			processor.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (CONNECTED.compareAndSet(this, 1, 2)) {
				processor.onError(t);
			}
			else {
				Exceptions.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (CONNECTED.compareAndSet(this, 1, 2)) {
				processor.onComplete();
			}
		}

		@Override
		public Object downstream() {
			return processor;
		}

	}

	final static class StateNormal<T, U>
			extends State<T, U> {

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StateNormal, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(StateNormal.class, Subscription.class, "s");

		public StateNormal(Processor<? super T, ? extends T> processor, Publisher<? extends U> publisher) {
		   super(processor, publisher);
		}

		@Override
		public void dispose() {
			if (CONNECTED.compareAndSet(this, 1, 2)) {
				if(SubscriptionHelper.terminate(S, this)) {
					processor.onError(new CancellationException("Disconnected"));
				}
			}
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.setOnce(S, this, s)) {
				processor.onSubscribe(s);
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			if(CONNECTED.compareAndSet(this, 1, 2)) {
				s.cancel();
			}
		}
	}

	final static class StateFuseable<T, U>
			extends State<T, U>
			implements Fuseable.QueueSubscription<T> {

		volatile Fuseable.QueueSubscription<T> s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StateFuseable, Fuseable.QueueSubscription> S =
				AtomicReferenceFieldUpdater.newUpdater(StateFuseable.class, Fuseable.QueueSubscription.class, "s");

		int sourceMode;

		public StateFuseable(Processor<? super T, ? extends T> processor, Publisher<? extends U> publisher) {
			super(processor, publisher);
		}

		@Override
		public void dispose() {
			if (CONNECTED.compareAndSet(this, 1, 2)) {
				Fuseable.QueueSubscription<?> a = this.s;
				if (a != null) {
					a = S.getAndSet(this, null);
					if (a != null) {
						a.cancel();
						processor.onError(new CancellationException("Disconnected"));
					}
				}
			}
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public void onSubscribe(Subscription s) {
			Fuseable.QueueSubscription<?> a = this.s;
			if (isTerminated()) {
				s.cancel();
				return;
			}
			if (a != null) {
				s.cancel();
				return;
			}

			if (S.compareAndSet(this, null, (Fuseable.QueueSubscription<?>)s)) {
				processor.onSubscribe(s);
			}
			else {
				s.cancel();
			}
		}


		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			if(CONNECTED.compareAndSet(this, 1, 2)) {
				s.cancel();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m = s.requestFusion(requestedMode);
			sourceMode = m;
			return m;
		}

		@Override
		public T poll() {
			if(isTerminated()){
				return null;
			}
			T v = s.poll();
			if(v == null && sourceMode == Fuseable.SYNC){
				CONNECTED.set(this, 2);
			}
			return v;
		}

		@Override
		public int size() {
			return s.size();
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			if(CONNECTED.compareAndSet(this, 1, 2)) {
				s.clear();
			}
		}
	}

}

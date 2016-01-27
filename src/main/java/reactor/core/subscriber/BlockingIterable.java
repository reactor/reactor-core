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
package reactor.core.subscriber;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import reactor.fn.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.state.Completable;
import reactor.core.util.CancelledSubscription;
import reactor.core.util.BackpressureUtils;

/**
 * An iterable that consumes a Publisher in a blocking fashion.
 * 
 * <p> It also implements methods to stream the contents via Stream
 * that also supports cancellation.
 *
 * @param <T> the value type
 */
public final class BlockingIterable<T> implements Iterable<T>, Receiver, Backpressurable {

	final Publisher<? extends T> source;
	
	final long batchSize;
	
	final Supplier<Queue<T>> queueSupplier;

	public BlockingIterable(Publisher<? extends T> source, long batchSize, Supplier<Queue<T>> queueSupplier) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("batchSize > 0 required but it was " + batchSize);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.batchSize = batchSize;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}
	
	@Override
	public Iterator<T> iterator() {
		SubscriberIterator<T> it = createIterator();
		
		source.subscribe(it);
		
		return it;
	}
	
	SubscriberIterator<T> createIterator() {
		Queue<T> q;
		
		try {
			q = queueSupplier.get();
		} catch (Throwable e) {
			throwError(e);
			return null;
		}
		
		if (q == null) {
			throw new NullPointerException("The queueSupplier returned a null queue");
		}
		
		return new SubscriberIterator<>(q, batchSize);
	}

	@Override
	public long getCapacity() {
		return batchSize;
	}

	@Override
	public Object upstream() {
		return source;
	}

	@Override
	public long getPending() {
		return 0;
	}

	static void throwError(Throwable e) {
		if (e instanceof RuntimeException) {
			throw (RuntimeException)e;
		}
		throw new RuntimeException(e);
	}
	
	static final class SubscriberIterator<T> implements Subscriber<T>, Iterator<T>, Runnable, Receiver, Completable {

		final Queue<T> queue;
		
		final long batchSize;
		
		final long limit;
		
		final Lock lock;
		
		final Condition condition;
		
		long produced;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SubscriberIterator, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SubscriberIterator.class, Subscription.class, "s");
		
		volatile boolean done;
		Throwable error;

		volatile boolean cancelled;
		
		public SubscriberIterator(Queue<T> queue, long batchSize) {
			this.queue = queue;
			this.batchSize = batchSize;
			this.limit = batchSize - (batchSize >> 2);
			this.lock = new ReentrantLock();
			this.condition = lock.newCondition();
		}

		@Override
		public boolean hasNext() {
			for (;;) {
				if (cancelled) {
					return false;
				}
				boolean d = done;
				boolean empty = queue.isEmpty();
				if (d) {
					Throwable e = error;
					if (e != null) {
						throwError(e);
						return false;
					} else
					if (empty) {
						return false;
					}
				}
				if (empty) {
					lock.lock();
					try {
						while (!cancelled && !done && queue.isEmpty()) {
							condition.await();
						}
					} catch (InterruptedException ex) {
						run();
						throwError(ex);
						return false;
					} finally {
						lock.unlock();
					}
				} else {
					return true;
				}
			}
		}

		@Override
		public T next() {
			if (hasNext()) {
				T v = queue.poll();
				
				if (v == null) {
					run();
					
					throw new IllegalStateException("Queue empty?!");
				}
				
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				} else {
					produced = p;
				}
				
				return v;
			}
			throw new NoSuchElementException();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				s.request(batchSize);
			}
		}

		@Override
		public void onNext(T t) {
			if (!queue.offer(t)) {
				BackpressureUtils.terminate(S, this);
				
				onError(new IllegalStateException("Queue full?!"));
			} else {
				signalConsumer();
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			signalConsumer();
		}

		@Override
		public void onComplete() {
			done = true;
			signalConsumer();
		}
		
		void signalConsumer() {
			lock.lock();
			try {
				condition.signalAll();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void run() {
			BackpressureUtils.terminate(S, this);
			signalConsumer();
		}
		
		@Override // otherwise default method which isn't available in Java 7
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public boolean isStarted() {
			return s != null && !done && s != CancelledSubscription.INSTANCE;
		}

		@Override
		public boolean isTerminated() {
			return done || s == CancelledSubscription.INSTANCE;
		}
	}

}

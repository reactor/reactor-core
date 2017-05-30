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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * An iterable that consumes a Publisher in a blocking fashion.
 * <p>
 * <p> It also implements methods to stream the contents via Stream
 * that also supports cancellation.
 *
 * @param <T> the value type
 */
final class BlockingIterable<T> implements Iterable<T>, Scannable {

	final Flux<? extends T> source;

	final long batchSize;

	final Supplier<Queue<T>> queueSupplier;

	BlockingIterable(Flux<? extends T> source,
			long batchSize,
			Supplier<Queue<T>> queueSupplier) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("batchSize > 0 required but it was " + batchSize);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.batchSize = batchSize;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == IntAttr.PREFETCH) return batchSize;
		if (key == ScannableAttr.PARENT) return source;

		return null;
	}

	@Override
	public Iterator<T> iterator() {
		SubscriberIterator<T> it = createIterator();

		source.subscribe(it);

		return it;
	}

	@Override
	public Spliterator<T> spliterator() {
		return stream().spliterator(); // cancellation should be composed through this way
	}

	/**
	 * @return a {@link Stream} of unknown size with onClose attached to {@link
	 * Subscription#cancel()}
	 */
	public Stream<T> stream() {
		SubscriberIterator<T> it = createIterator();
		source.subscribe(it);

		Spliterator<T> sp = Spliterators.spliteratorUnknownSize(it, 0);

		return StreamSupport.stream(sp, false)
		                    .onClose(it);
	}

	SubscriberIterator<T> createIterator() {
		Queue<T> q;

		try {
			q = Objects.requireNonNull(queueSupplier.get(),
					"The queueSupplier returned a null queue");
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}

		return new SubscriberIterator<>(q, batchSize);
	}

	static final class SubscriberIterator<T>
			implements InnerConsumer<T>, Iterator<T>, Runnable {

		final Queue<T> queue;

		final long batchSize;

		final long limit;

		final Lock lock;

		final Condition condition;

		long produced;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SubscriberIterator, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SubscriberIterator.class,
						Subscription.class,
						"s");

		volatile boolean done;
		Throwable error;

		 SubscriberIterator(Queue<T> queue, long batchSize) {
			this.queue = queue;
			this.batchSize = batchSize;
			this.limit = batchSize - (batchSize >> 2);
			this.lock = new ReentrantLock();
			this.condition = lock.newCondition();
		}

		@Override
		public boolean hasNext() {
			for (; ; ) {
				boolean d = done;
				boolean empty = queue.isEmpty();
				if (d) {
					Throwable e = error;
					if (e != null) {
						throw Exceptions.propagate(e);
					}
					else if (empty) {
						return false;
					}
				}
				if (empty) {
					lock.lock();
					try {
						while (!done && queue.isEmpty()) {
							condition.await();
						}
					}
					catch (InterruptedException ex) {
						run();
						throw Exceptions.propagate(ex);
					}
					finally {
						lock.unlock();
					}
				}
				else {
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

					throw new IllegalStateException("Queue is empty: Expected one element to be available from the Reactive Streams source.");
				}

				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}

				return v;
			}
			throw new NoSuchElementException();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(batchSize);
			}
		}

		@Override
		public void onNext(T t) {
			if (!queue.offer(t)) {
				Operators.terminate(S, this);

				onError(Operators.onOperatorError(null,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t));
			}
			else {
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
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void run() {
			Operators.terminate(S, this);
			signalConsumer();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == ScannableAttr.PARENT) return  s;
			if (key == BooleanAttr.CANCELLED) 			    	return s == Operators.cancelledSubscription();
			if (key == IntAttr.PREFETCH) return batchSize;
			if (key == ThrowableAttr.ERROR) return error;

			return null;
		}
	}

}

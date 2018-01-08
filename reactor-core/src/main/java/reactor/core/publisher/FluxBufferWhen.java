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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.MpscLinkedQueue;

/**
 * buffers elements into possibly overlapping buffers whose boundaries are determined
 * by a start Publisher's element and a signal of a derived Publisher
 *
 * @param <T> the source value type
 * @param <OPEN> the value type of the publisher opening the buffers
 * @param <CLOSE> the value type of the publisher closing the individual buffers
 * @param <BUFFER> the collection type that holds the buffered values
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBufferWhen<T, OPEN, CLOSE, BUFFER extends Collection<? super T>>
		extends FluxOperator<T, BUFFER> {

	final Publisher<OPEN> start;

	final Function<? super OPEN, ? extends Publisher<CLOSE>> end;

	final Supplier<BUFFER> bufferSupplier;

	final Supplier<? extends Queue<BUFFER>> queueSupplier;

	FluxBufferWhen(Flux<? extends T> source,
			Publisher<OPEN> start,
			Function<? super OPEN, ? extends Publisher<CLOSE>> end,
			Supplier<BUFFER> bufferSupplier,
			Supplier<? extends Queue<BUFFER>> queueSupplier) {
		super(source);
		this.start = Objects.requireNonNull(start, "start");
		this.end = Objects.requireNonNull(end, "end");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void subscribe(CoreSubscriber<? super BUFFER> actual) {
		BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER> main =
				new BufferWhenMainSubscriber<>(actual, bufferSupplier, end);

		actual.onSubscribe(main);

		BufferWhenOpenSubscriber<T, OPEN, CLOSE, BUFFER> bos =
				new BufferWhenOpenSubscriber<>(main);
		if (main.resources.add(bos)) {
			main.bos = bos; //keep reference to remove and dispose it if source early onCompletes

			BufferWhenMainSubscriber.WINDOWS.lazySet(main, 1);
			start.subscribe(bos);

			source.subscribe(main);
		}
	}

	static final class BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER extends Collection<? super T>>
			extends QueueDrainSubscriber<T, BUFFER, BUFFER>
			implements Disposable {
		final Function<? super OPEN, ? extends Publisher<? extends CLOSE>> bufferClose;
		final Supplier<BUFFER>                                             bufferSupplier;
		final Composite                                                    resources;

		Subscription s;

		final List<BUFFER> buffers;

		volatile int windows;
		static final AtomicIntegerFieldUpdater<BufferWhenMainSubscriber> WINDOWS =
				AtomicIntegerFieldUpdater.newUpdater(BufferWhenMainSubscriber.class, "windows");

		BufferWhenOpenSubscriber<T, OPEN, CLOSE, BUFFER> bos;

		BufferWhenMainSubscriber(CoreSubscriber<? super BUFFER> actual,
				Supplier<BUFFER> bufferSupplier,
				Function<? super OPEN, ? extends Publisher<? extends CLOSE>> bufferClose) {
			super(actual, new MpscLinkedQueue<>());
			this.bufferClose = bufferClose;
			this.bufferSupplier = bufferSupplier;
			this.buffers = new LinkedList<>();
			this.resources = Disposables.composite();
		}
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (this) {
				for (BUFFER b : buffers) {
					b.add(t);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			cancel();
			cancelled = true;
			synchronized (this) {
				buffers.clear();
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			done = true;
			resources.remove(bos);
			bos.dispose();
			if (WINDOWS.decrementAndGet(this) == 0) {
				complete();
			}
		}

		void complete() {
			List<BUFFER> list;
			synchronized (this) {
				list = new ArrayList<>(buffers);
				buffers.clear();
			}

			Queue<BUFFER> q = queue;
			for (BUFFER u : list) {
				q.offer(u);
			}
			done = true;
			if (enter()) {
				drainMaxLoop(q, actual, false, this, this);
			}
		}

		@Override
		public void request(long n) {
			requested(n);
		}

		@Override
		public void dispose() {
			resources.dispose();
		}

		@Override
		public boolean isDisposed() {
			return resources.isDisposed();
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				dispose();
			}
		}

		@Override
		public boolean accept(Subscriber<? super BUFFER> a, BUFFER v) {
			a.onNext(v);
			return true;
		}

		void open(OPEN window) {
			if (cancelled) {
				return;
			}

			BUFFER b;

			try {
				b = Objects.requireNonNull(bufferSupplier.get(), "The buffer supplied is null");
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}

			Publisher<? extends CLOSE> p;

			try {
				p = Objects.requireNonNull(bufferClose.apply(window), "The buffer closing publisher is null");
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}

			if (cancelled) {
				return;
			}

			synchronized (this) {
				if (cancelled) {
					return;
				}
				buffers.add(b);
			}

			BufferWhenCloseSubscriber<T, OPEN, CLOSE, BUFFER> bcs =
					new BufferWhenCloseSubscriber<>(b, this);
			resources.add(bcs);

			WINDOWS.getAndIncrement(this);

			p.subscribe(bcs);
		}

		void openFinished(Disposable d) {
			if (resources.remove(d)) {
				if (WINDOWS.decrementAndGet(this) == 0) {
					complete();
				}
			}
		}

		void close(BUFFER b, Disposable d) {
			boolean e;
			synchronized (this) {
				e = buffers.remove(b);
			}

			if (e) {
				fastPathOrderedEmitMax(b, false, this);
			}

			if (resources.remove(d)) {
				if (WINDOWS.decrementAndGet(this) == 0) {
					complete();
				}
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.BUFFERED) return buffers.stream()
			                                        .mapToInt(Collection::size)
			                                        .sum();

			return super.scanUnsafe(key);
		}
	}

	static final class BufferWhenOpenSubscriber<T, OPEN, CLOSE, BUFFER extends Collection<? super T>>
			implements Disposable, InnerConsumer<OPEN> {

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<BufferWhenOpenSubscriber, Subscription> SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(BufferWhenOpenSubscriber.class, Subscription.class, "subscription");

		final BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER> parent;
		boolean done;

		BufferWhenOpenSubscriber(BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				subscription.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		public boolean isDisposed() {
			return subscription == Operators.cancelledSubscription();
		}

		@Override
		public void onNext(OPEN t) {
			if (done) {
				return;
			}
			parent.open(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}
			done = true;
			parent.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			parent.openFinished(this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return parent;
			}
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return Long.MAX_VALUE;
			if (key == Attr.CANCELLED) return isDisposed();

			return null;
		}
	}

	static final class BufferWhenCloseSubscriber<T, OPEN, CLOSE, BUFFER extends Collection<? super T>>
			implements Disposable, InnerConsumer<CLOSE> {

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<BufferWhenCloseSubscriber, Subscription> SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(BufferWhenCloseSubscriber.class, Subscription.class, "subscription");

		final BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER> parent;
		final BUFFER                                           value;
		boolean done;


		BufferWhenCloseSubscriber(BUFFER value, BufferWhenMainSubscriber<T, OPEN, CLOSE, BUFFER> parent) {
			this.parent = parent;
			this.value = value;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				subscription.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		public boolean isDisposed() {
			return subscription == Operators.cancelledSubscription();
		}

		@Override
		public void onNext(CLOSE t) {
			onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}
			parent.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			parent.close(value, this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return parent;
			}
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return Long.MAX_VALUE;
			if (key == Attr.CANCELLED) return isDisposed();

			return null;
		}
	}
}

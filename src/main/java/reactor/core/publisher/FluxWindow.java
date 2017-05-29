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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;

/**
 * Splits the source sequence into possibly overlapping publishers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxWindow<T> extends FluxSource<T, Flux<T>> {

	final int size;

	final int skip;

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	final Supplier<? extends Queue<UnicastProcessor<T>>> overflowQueueSupplier;

	FluxWindow(Flux<? extends T> source,
			int size,
			Supplier<? extends Queue<T>> processorQueueSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		this.size = size;
		this.skip = size;
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.overflowQueueSupplier = null; // won't be needed here
	}

	FluxWindow(Flux<? extends T> source,
			int size,
			int skip,
			Supplier<? extends Queue<T>> processorQueueSupplier,
			Supplier<? extends Queue<UnicastProcessor<T>>> overflowQueueSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		if (skip <= 0) {
			throw new IllegalArgumentException("skip > 0 required but it was " + skip);
		}
		this.size = size;
		this.skip = skip;
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.overflowQueueSupplier =
				Objects.requireNonNull(overflowQueueSupplier, "overflowQueueSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super Flux<T>> s) {
		if (skip == size) {
			source.subscribe(new WindowExactSubscriber<>(s,
					size,
					processorQueueSupplier));
		}
		else if (skip > size) {
			source.subscribe(new WindowSkipSubscriber<>(s,
					size,
					skip,
					processorQueueSupplier));
		}
		else {
			source.subscribe(new WindowOverlapSubscriber<>(s,
					size,
					skip,
					processorQueueSupplier,
					overflowQueueSupplier.get()));
		}
	}

	static final class WindowExactSubscriber<T>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final Subscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final int size;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowExactSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowExactSubscriber.class, "cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowExactSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowExactSubscriber.class, "windowCount");

		int index;

		Subscription s;

		UnicastProcessor<T> window;

		boolean done;

		WindowExactSubscriber(Subscriber<? super Flux<T>> actual,
				int size,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.size = size;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
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
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			int i = index;

			UnicastProcessor<T> w = window;
			if (cancelled == 0 && i == 0) {
				WINDOW_COUNT.getAndIncrement(this);

				w = new UnicastProcessor<>(processorQueueSupplier.get(), this);
				window = w;

				actual.onNext(w);
			}

			i++;

			w.onNext(t);

			if (i == size) {
				index = 0;
				window = null;
				w.onComplete();
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long u = Operators.multiplyCap(size, n);
				s.request(u);
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public Subscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled == 1;
			if (key == IntAttr.CAPACITY) return size;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}
	}

	static final class WindowSkipSubscriber<T>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final Subscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final int size;

		final int skip;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class, "cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class, "windowCount");

		volatile int firstRequest;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> FIRST_REQUEST =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class,
						"firstRequest");

		int index;

		Subscription s;

		UnicastProcessor<T> window;

		boolean done;

		WindowSkipSubscriber(Subscriber<? super Flux<T>> actual,
				int size,
				int skip,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
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
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			int i = index;

			UnicastProcessor<T> w = window;
			if (i == 0) {
				WINDOW_COUNT.getAndIncrement(this);

				w = new UnicastProcessor<>(processorQueueSupplier.get(), this);
				window = w;

				actual.onNext(w);
			}

			i++;

			if (w != null) {
				w.onNext(t);
			}

			if (i == size) {
				window = null;
				if (w != null) {
					w.onComplete();
				}
			}

			if (i == skip) {
				index = 0;
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;

			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
					long u = Operators.multiplyCap(size, n);
					long v = Operators.multiplyCap(skip - size, n - 1);
					long w = Operators.addCap(u, v);
					s.request(w);
				}
				else {
					long u = Operators.multiplyCap(skip, n);
					s.request(u);
				}
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public Subscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled == 1;
			if (key == IntAttr.CAPACITY) return size;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}
	}

	static final class WindowOverlapSubscriber<T> extends ArrayDeque<UnicastProcessor<T>>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final Subscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final Queue<UnicastProcessor<T>> queue;

		final int size;

		final int skip;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"windowCount");

		volatile int firstRequest;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> FIRST_REQUEST =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"firstRequest");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowOverlapSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class, "wip");

		int index;

		int produced;

		Subscription s;

		volatile boolean done;
		Throwable error;

		WindowOverlapSubscriber(Subscriber<? super Flux<T>> actual,
				int size,
				int skip,
				Supplier<? extends Queue<T>> processorQueueSupplier,
				Queue<UnicastProcessor<T>> overflowQueue) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
			this.queue = overflowQueue;
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
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			int i = index;

			if (i == 0) {
				if (cancelled == 0) {
					WINDOW_COUNT.getAndIncrement(this);

					UnicastProcessor<T> w = new UnicastProcessor<>(processorQueueSupplier.get(), this);

					offer(w);

					queue.offer(w);
					drain();
				}
			}

			i++;

			for (Processor<T, T> w : this) {
				w.onNext(t);
			}

			int p = produced + 1;
			if (p == size) {
				produced = p - skip;

				Processor<T, T> w = poll();
				if (w != null) {
					w.onComplete();
				}
			}
			else {
				produced = p;
			}

			if (i == skip) {
				index = 0;
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;

			for (Processor<T, T> w : this) {
				w.onError(t);
			}
			clear();

			error = t;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			for (Processor<T, T> w : this) {
				w.onComplete();
			}
			clear();

			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super Flux<T>> a = actual;
			final Queue<UnicastProcessor<T>> q = queue;
			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0;

				while (e != r) {
					boolean d = done;

					UnicastProcessor<T> t = q.poll();

					boolean empty = t == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled == 1) {
				q.clear();
				return true;
			}

			if (d) {
				Throwable e = error;

				if (e != null) {
					q.clear();
					a.onError(e);
					return true;
				}
				else if (empty) {
					a.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {

				Operators.getAndAddCap(REQUESTED, this, n);

				if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
					long u = Operators.multiplyCap(skip, n - 1);
					long v = Operators.addCap(size, u);
					s.request(v);
				}
				else {
					long u = Operators.multiplyCap(skip, n);
					s.request(u);
				}

				drain();
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public Subscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled == 1;
			if (key == IntAttr.CAPACITY) return size;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.BUFFERED) return queue.size() + size();
			if (key == ThrowableAttr.ERROR) return error;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(toArray())
			             .map(Scannable::from);
		}
	}

}

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
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Buffers a certain number of subsequent elements and emits the buffers.
 *
 * @param <T> the source value type
 * @param <C> the buffer collection type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBuffer<T, C extends Collection<? super T>> extends FluxSource<T, C> {

	final int size;

	final int skip;

	final Supplier<C> bufferSupplier;

	FluxBuffer(Publisher<? extends T> source, int size, Supplier<C> bufferSupplier) {
		this(source, size, size, bufferSupplier);
	}

	FluxBuffer(Publisher<? extends T> source,
			int size,
			int skip,
			Supplier<C> bufferSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}

		if (skip <= 0) {
			throw new IllegalArgumentException("skip > 0 required but it was " + size);
		}

		this.size = size;
		this.skip = skip;
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super C> s) {
		if (size == skip) {
			source.subscribe(new BufferExactSubscriber<>(s, size, bufferSupplier));
		}
		else if (skip > size) {
			source.subscribe(new BufferSkipSubscriber<>(s, size, skip, bufferSupplier));
		}
		else {
			source.subscribe(new BufferOverlappingSubscriber<>(s,
					size,
					skip,
					bufferSupplier));
		}
	}

	static final class BufferExactSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		C buffer;

		Subscription s;

		boolean done;

		BufferExactSubscriber(Subscriber<? super C> actual,
				int size,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				s.request(Operators.multiplyCap(n, size));
			}
		}

		@Override
		public void cancel() {
			s.cancel();
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

			C b = buffer;
			if (b == null) {
				try {
					b = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
				buffer = b;
			}

			b.add(t);

			if (b.size() == size) {
				buffer = null;
				actual.onNext(b);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			C b = buffer;

			if (b != null && !b.isEmpty()) {
				actual.onNext(b);
			}
			actual.onComplete();
		}

		@Override
		public Subscriber<? super C> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.CAPACITY) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == IntAttr.PREFETCH) return size;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class BufferSkipSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		final int skip;

		C buffer;

		Subscription s;

		boolean done;

		long index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferSkipSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BufferSkipSubscriber.class, "wip");

		BufferSkipSubscriber(Subscriber<? super C> actual,
				int size,
				int skip,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public void request(long n) {
			if (!Operators.validate(n)) {
				return;
			}

			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				// n full buffers
				long u = Operators.multiplyCap(n, size);
				// + (n - 1) gaps
				long v = Operators.multiplyCap(skip - size, n - 1);

				s.request(Operators.addCap(u, v));
			}
			else {
				// n full buffer + gap
				s.request(Operators.multiplyCap(skip, n));
			}
		}

		@Override
		public void cancel() {
			s.cancel();
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

			C b = buffer;

			long i = index;

			if (i % skip == 0L) {
				try {
					b = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}

				buffer = b;
			}

			if (b != null) {
				b.add(t);
				if (b.size() == size) {
					buffer = null;
					actual.onNext(b);
				}
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}

			done = true;
			buffer = null;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;
			C b = buffer;
			buffer = null;

			if (b != null) {
				actual.onNext(b);
			}

			actual.onComplete();
		}

		@Override
		public Subscriber<? super C> actual() {
			return actual;
		}

		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.CAPACITY) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == IntAttr.PREFETCH) return size;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class BufferOverlappingSubscriber<T, C extends Collection<? super T>>
			extends ArrayDeque<C>
			implements BooleanSupplier, InnerOperator<T, C> {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		final int skip;

		Subscription s;

		boolean done;

		long index;

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferOverlappingSubscriber> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(BufferOverlappingSubscriber.class,
						"once");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferOverlappingSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferOverlappingSubscriber.class,
						"requested");

		BufferOverlappingSubscriber(Subscriber<? super C> actual,
				int size,
				int skip,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public void request(long n) {

			if (!Operators.validate(n)) {
				return;
			}

			if (DrainUtils.postCompleteRequest(n,
					actual,
					this,
					REQUESTED,
					this,
					this)) {
				return;
			}

			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
				// (n - 1) skips
				long u = Operators.multiplyCap(skip, n - 1);

				// + 1 full buffer
				long r = Operators.addCap(size, u);
				s.request(r);
			}
			else {
				// n skips
				long r = Operators.multiplyCap(skip, n);
				s.request(r);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			s.cancel();
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

			long i = index;

			if (i % skip == 0L) {
				C b;

				try {
					b = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}

				offer(b);
			}

			C b = peek();

			if (b != null && b.size() + 1 == size) {
				poll();

				b.add(t);

				actual.onNext(b);

				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
			}

			for (C b0 : this) {
				b0.add(t);
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}

			done = true;
			clear();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;

			DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public Subscriber<? super C> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.CAPACITY) return size() * size;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

	}
}

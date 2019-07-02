/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Buffers a certain number of subsequent elements and emits the buffers.
 *
 * @param <T> the source value type
 * @param <C> the buffer collection type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBuffer<T, C extends Collection<? super T>> extends InternalFluxOperator<T, C> {

	final int size;

	final int skip;

	final Supplier<C> bufferSupplier;

	FluxBuffer(Flux<? extends T> source, int size, Supplier<C> bufferSupplier) {
		this(source, size, size, bufferSupplier);
	}

	FluxBuffer(Flux<? extends T> source,
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
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		if (size == skip) {
			return new BufferExactSubscriber<>(actual, size, bufferSupplier);
		}
		else if (skip > size) {
			return new BufferSkipSubscriber<>(actual, size, skip, bufferSupplier);
		}
		else {
			return new BufferOverlappingSubscriber<>(actual, size, skip, bufferSupplier);
		}
	}

	static final class BufferExactSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final CoreSubscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		C buffer;

		Subscription s;

		boolean done;

		BufferExactSubscriber(CoreSubscriber<? super C> actual,
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
			Operators.onDiscardMultiple(buffer, actual.currentContext());
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
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			C b = buffer;
			if (b == null) {
				try {
					b = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
				}
				catch (Throwable e) {
					Context ctx = actual.currentContext();
					onError(Operators.onOperatorError(s, e, t, ctx));
					Operators.onDiscard(t, ctx); //this is in no buffer
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
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			actual.onError(t);
			Operators.onDiscardMultiple(buffer, actual.currentContext());
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
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == Attr.CAPACITY) return size;
			if (key == Attr.PREFETCH) return size;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class BufferSkipSubscriber<T, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final CoreSubscriber<? super C> actual;
		final Context ctx;

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

		BufferSkipSubscriber(CoreSubscriber<? super C> actual,
				int size,
				int skip,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.ctx = actual.currentContext();
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
			Operators.onDiscardMultiple(buffer, this.ctx);
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
				Operators.onNextDropped(t, this.ctx);
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
					onError(Operators.onOperatorError(s, e, t, this.ctx));
					Operators.onDiscard(t, this.ctx); //t hasn't got a chance to end up in any buffer
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
			else {
				//dropping
				Operators.onDiscard(t, this.ctx);
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, this.ctx);
				return;
			}

			done = true;
			C b = buffer;
			buffer = null;

			actual.onError(t);
			Operators.onDiscardMultiple(b, this.ctx);
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
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CAPACITY) return size;
			if (key == Attr.BUFFERED) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == Attr.PREFETCH) return size;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class BufferOverlappingSubscriber<T, C extends Collection<? super T>>
			extends ArrayDeque<C>
			implements BooleanSupplier, InnerOperator<T, C> {

		final CoreSubscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		final int skip;

		Subscription s;

		boolean done;

		long index;

		volatile boolean cancelled;

		long produced;

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

		BufferOverlappingSubscriber(CoreSubscriber<? super C> actual,
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
			clear();
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
				Operators.onNextDropped(t, actual.currentContext());
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
					Context ctx = actual.currentContext();
					onError(Operators.onOperatorError(s, e, t, ctx));
					Operators.onDiscard(t, ctx); //didn't get a chance to be added to a buffer
					return;
				}

				offer(b);
			}

			C b = peek();

			if (b != null && b.size() + 1 == size) {
				poll();

				b.add(t);

				actual.onNext(b);

				produced++;
			}

			for (C b0 : this) {
				b0.add(t);
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			done = true;
			clear();

			actual.onError(t);
		}

		@Override
		public void clear() {
			Context ctx = actual.currentContext();
            for(C b: this) {
            	Operators.onDiscardMultiple(b, ctx);
            }
			super.clear();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;
			long p = produced;
			if (p != 0L) {
				Operators.produced(REQUESTED,this, p);
			}
			DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.CAPACITY) return size() * size;
			if (key == Attr.BUFFERED) return stream().mapToInt(Collection::size).sum();
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

	}
}

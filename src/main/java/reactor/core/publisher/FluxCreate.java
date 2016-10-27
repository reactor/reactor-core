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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.Producer;
import reactor.core.Trackable;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.util.concurrent.QueueSupplier;

/**
 * Provides a multi-valued sink API for a callback that is called for
 * each individual Subscriber.
 *
 * @param <T> the value type
 */
final class FluxCreate<T> extends Flux<T> {

	final Consumer<? super FluxSink<T>> source;

	final OverflowStrategy backpressure;

	public FluxCreate(Consumer<? super FluxSink<T>> source,
			FluxSink.OverflowStrategy backpressure) {
		this.source = Objects.requireNonNull(source, "source");
		this.backpressure = Objects.requireNonNull(backpressure, "backpressure");
	}

	@Override
	public void subscribe(Subscriber<? super T> t) {
		BaseSink<T> sink;

		switch (backpressure) {
			case IGNORE: {
				sink = new IgnoreSink<>(t);
				break;
			}
			case ERROR: {
				sink = new ErrorAsyncSink<>(t);
				break;
			}
			case DROP: {
				sink = new DropAsyncSink<>(t);
				break;
			}
			case LATEST: {
				sink = new LatestAsyncSink<>(t);
				break;
			}
			default: {
				sink = new BufferAsyncSink<>(t, QueueSupplier.SMALL_BUFFER_SIZE);
				break;
			}
		}

		t.onSubscribe(sink);
		try {
			source.accept(sink);
		}
		catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			sink.error(Operators.onOperatorError(ex));
		}
	}

	/**
	 * Serializes calls to onNext, onError and onComplete.
	 *
	 * @param <T> the value type
	 */
	static final class SerializedSink<T> implements FluxSink<T> {

		final BaseSink<T> sink;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SerializedSink, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SerializedSink.class, Throwable.class, "error");


		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SerializedSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SerializedSink.class, "wip");


		final Queue<T> queue;

		volatile boolean done;

		public SerializedSink(BaseSink<T> sink) {
			this.sink = sink;
			this.queue = QueueSupplier.<T>unbounded(QueueSupplier.XS_BUFFER_SIZE).get();
		}

		@Override
		public void next(T t) {
			if (sink.isCancelled() || done) {
				return;
			}
			if (t == null) {
				throw new NullPointerException("t is null in sink.next(t)");
			}
			if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
				sink.next(t);
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
			}
			else {
				Queue<T> q = queue;
				synchronized (this) {
					q.offer(t);
				}
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
		}

		@Override
		public void error(Throwable t) {
			if (sink.isCancelled() || done) {
				Operators.onErrorDropped(t);
				return;
			}
			if (t == null) {
				throw new NullPointerException("t is null in sink.error(t)");
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void complete() {
			if (sink.isCancelled() || done) {
				return;
			}
			done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) == 0) {
				drainLoop();
			}
		}

		void drainLoop() {
			BaseSink<T> e = sink;
			Queue<T> q = queue;
			int missed = 1;
			for (; ; ) {

				for (; ; ) {
					if (e.isCancelled()) {
						q.clear();
						return;
					}

					if (ERROR.get(this) != null) {
						q.clear();
						e.error(Exceptions.terminate(ERROR, this));
						return;
					}

					boolean d = done;
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						// should never happen
						v = null;
					}

					boolean empty = v == null;

					if (d && empty) {
						e.complete();
						return;
					}

					if (empty) {
						break;
					}

					e.next(v);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void setCancellation(Cancellation c) {
			sink.setCancellation(c);
		}

		@Override
		public long requestedFromDownstream() {
			return sink.requestedFromDownstream();
		}

		@Override
		public boolean isCancelled() {
			return sink.isCancelled();
		}

		@Override
		public FluxSink<T> serialize() {
			return this;
		}
	}

	static abstract class BaseSink<T>
			implements FluxSink<T>, Subscription, Trackable, Producer {

		final Subscriber<? super T> actual;

		volatile Cancellation cancel;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BaseSink, Cancellation> CANCEL =
				AtomicReferenceFieldUpdater.newUpdater(BaseSink.class,
						Cancellation.class,
						"cancel");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BaseSink> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BaseSink.class, "requested");

		static final Cancellation CANCELLED = () -> {
		};

		public BaseSink(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void complete() {
			if (isCancelled()) {
				return;
			}
			try {
				actual.onComplete();
			}
			finally {
				cancelResource();
			}
		}

		@Override
		public void error(Throwable e) {
			if (isCancelled()) {
				return;
			}
			try {
				actual.onError(e);
			}
			finally {
				cancelResource();
			}
		}

		@Override
		public final void cancel() {
			cancelResource();
			onCancel();
		}

		void cancelResource() {
			Cancellation c = cancel;
			if (c != CANCELLED) {
				c = CANCEL.getAndSet(this, CANCELLED);
				if (c != null && c != CANCELLED) {
					c.dispose();
				}
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		void onCancel() {
			// default is no-op
		}

		@Override
		public final boolean isCancelled() {
			return cancel == CANCELLED;
		}

		@Override
		public final void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				onRequestedFromDownstream();
			}
		}

		void onRequestedFromDownstream() {
			// default is no-op
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}

		@Override
		public final void setCancellation(Cancellation c) {
			if (!CANCEL.compareAndSet(this, null, c)) {
				if (cancel != CANCELLED && c != null) {
					c.dispose();
				}
			}
		}

		@Override
		public final FluxSink<T> serialize() {
			return new SerializedSink<>(this);
		}
	}

	static final class IgnoreSink<T> extends BaseSink<T> {

		public IgnoreSink(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public void next(T t) {
			if (isCancelled()) {
				return;
			}

			actual.onNext(t);

			for (; ; ) {
				long r = requested;
				if (r == 0L || REQUESTED.compareAndSet(this, r, r - 1)) {
					return;
				}
			}
		}

	}

	static abstract class NoOverflowBaseAsyncSink<T> extends BaseSink<T> {

		public NoOverflowBaseAsyncSink(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public final void next(T t) {
			if (isCancelled()) {
				return;
			}

			if (requested != 0) {
				actual.onNext(t);
				Operators.produced(REQUESTED, this, 1);
			}
			else {
				onOverflow();
			}
		}

		abstract void onOverflow();
	}

	static final class DropAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		public DropAsyncSink(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void onOverflow() {
			// nothing to do
		}

	}

	static final class ErrorAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		public ErrorAsyncSink(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void onOverflow() {
			error(Exceptions.failWithOverflow());
		}

	}

	static final class BufferAsyncSink<T> extends BaseSink<T> {

		final Queue<T> queue;

		Throwable error;
		volatile boolean done;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferAsyncSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BufferAsyncSink.class, "wip");

		public BufferAsyncSink(Subscriber<? super T> actual, int capacityHint) {
			super(actual);
			this.queue = QueueSupplier.<T>unbounded(capacityHint).get();
		}

		@Override
		public void next(T t) {
			queue.offer(t);
			drain();
		}

		@Override
		public void error(Throwable e) {
			error = e;
			done = true;
			drain();
		}

		@Override
		public void complete() {
			done = true;
			drain();
		}

		@Override
		void onRequestedFromDownstream() {
			drain();
		}

		@Override
		void onCancel() {
			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			for (; ; ) {
				long r = requested;
				long e = 0L;

				while (e != r) {
					if (isCancelled()) {
						q.clear();
						return;
					}

					boolean d = done;

					T o = q.poll();

					boolean empty = o == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(o);

					e++;
				}

				if (e == r) {
					if (isCancelled()) {
						q.clear();
						return;
					}

					boolean d = done;

					boolean empty = q.isEmpty();

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}
				}

				if (e != 0) {
					Operators.produced(REQUESTED, this, e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
	}

	static final class LatestAsyncSink<T> extends BaseSink<T> {

		final AtomicReference<T> queue;

		Throwable error;
		volatile boolean done;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<LatestAsyncSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(LatestAsyncSink.class, "wip");


		public LatestAsyncSink(Subscriber<? super T> actual) {
			super(actual);
			this.queue = new AtomicReference<>();
		}

		@Override
		public void next(T t) {
			queue.set(t);
			drain();
		}

		@Override
		public void error(Throwable e) {
			error = e;
			done = true;
			drain();
		}

		@Override
		public void complete() {
			done = true;
			drain();
		}

		@Override
		void onRequestedFromDownstream() {
			drain();
		}

		@Override
		void onCancel() {
			if (WIP.getAndIncrement(this) == 0) {
				queue.lazySet(null);
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			final Subscriber<? super T> a = actual;
			final AtomicReference<T> q = queue;

			for (; ; ) {
				long r = requested;
				long e = 0L;

				while (e != r) {
					if (isCancelled()) {
						q.lazySet(null);
						return;
					}

					boolean d = done;

					T o = q.getAndSet(null);

					boolean empty = o == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(o);

					e++;
				}

				if (e == r) {
					if (isCancelled()) {
						q.lazySet(null);
						return;
					}

					boolean d = done;

					boolean empty = q.get() == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}
				}

				if (e != 0) {
					Operators.produced(REQUESTED, this, e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
	}
}

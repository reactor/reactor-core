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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Provides a multi-valued sink API for a callback that is called for each individual
 * Subscriber.
 *
 * @param <T> the value type
 */
final class FluxCreate<T> extends Flux<T> {

	enum CreateMode {
		PUSH_ONLY, PUSH_PULL
	}

	final Consumer<? super FluxSink<T>> source;

	final OverflowStrategy backpressure;

	final CreateMode createMode;

	FluxCreate(Consumer<? super FluxSink<T>> source,
			FluxSink.OverflowStrategy backpressure,
			CreateMode createMode) {
		this.source = Objects.requireNonNull(source, "source");
		this.backpressure = Objects.requireNonNull(backpressure, "backpressure");
		this.createMode = createMode;
	}

	static <T> BaseSink<T> createSink(CoreSubscriber<? super T> t,
			OverflowStrategy backpressure) {
		switch (backpressure) {
			case IGNORE: {
				return new IgnoreSink<>(t);
			}
			case ERROR: {
				return new ErrorAsyncSink<>(t);
			}
			case DROP: {
				return new DropAsyncSink<>(t);
			}
			case LATEST: {
				return new LatestAsyncSink<>(t);
			}
			default: {
				return new BufferAsyncSink<>(t, Queues.SMALL_BUFFER_SIZE);
			}
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		BaseSink<T> sink = createSink(actual, backpressure);

		actual.onSubscribe(sink);
		try {
			source.accept(
					createMode == CreateMode.PUSH_PULL ? new SerializedSink<>(sink) :
							sink);
		}
		catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			sink.error(Operators.onOperatorError(ex, actual.currentContext()));
		}
	}

	/**
	 * Serializes calls to onNext, onError and onComplete.
	 *
	 * @param <T> the value type
	 */
	static final class SerializedSink<T> implements FluxSink<T>, Scannable {

		final BaseSink<T> sink;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SerializedSink, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SerializedSink.class,
						Throwable.class,
						"error");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SerializedSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SerializedSink.class, "wip");

		final Queue<T> queue;

		volatile boolean done;

		SerializedSink(BaseSink<T> sink) {
			this.sink = sink;
			this.queue = Queues.<T>unbounded(16).get();
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		public FluxSink<T> next(T t) {
			if (sink.isCancelled() || done) {
				Operators.onNextDropped(t, sink.currentContext());
				return this;
			}
			//noinspection ConstantConditions
			if (t == null) {
				throw new NullPointerException("t is null in sink.next(t)");
			}
			if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
				sink.next(t);
				if (WIP.decrementAndGet(this) == 0) {
					return this;
				}
			}
			else {
				Queue<T> q = queue;
				synchronized (this) {
					q.offer(t);
				}
				if (WIP.getAndIncrement(this) != 0) {
					return this;
				}
			}
			drainLoop();
			return this;
		}

		@Override
		public void error(Throwable t) {
			if (sink.isCancelled() || done) {
				Operators.onErrorDropped(t, sink.currentContext());
				return;
			}
			//noinspection ConstantConditions
			if (t == null) {
				throw new NullPointerException("t is null in sink.error(t)");
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t, sink.currentContext());
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
						//noinspection ConstantConditions
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
		public FluxSink<T> onRequest(LongConsumer consumer) {
			sink.onRequest(consumer, consumer, sink.requested);
			return this;
		}

		@Override
		public FluxSink<T> onCancel(Disposable d) {
			sink.onCancel(d);
			return this;
		}

		@Override
		public FluxSink<T> onDispose(Disposable d) {
			sink.onDispose(d);
			return this;
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return queue.size();
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}

			return sink.scanUnsafe(key);
		}
	}

	/**
	 * Serializes calls to onNext, onError and onComplete if onRequest is invoked.
	 * Otherwise, non-serialized base sink is used.
	 *
	 * @param <T> the value type
	 */
	static class SerializeOnRequestSink<T> implements FluxSink<T>, Scannable {

		final BaseSink<T> baseSink;
		SerializedSink<T> serializedSink;
		FluxSink<T>       sink;

		SerializeOnRequestSink(BaseSink<T> sink) {
			this.baseSink = sink;
			this.sink = sink;
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return serializedSink != null ? serializedSink.scanUnsafe(key) :
					baseSink.scanUnsafe(key);
		}

		@Override
		public void complete() {
			sink.complete();
		}

		@Override
		public void error(Throwable e) {
			sink.error(e);
		}

		@Override
		public FluxSink<T> next(T t) {
			sink.next(t);
			return serializedSink == null ? this : serializedSink;
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
		public FluxSink<T> onRequest(LongConsumer consumer) {
			if (serializedSink == null) {
				serializedSink = new SerializedSink<>(baseSink);
				sink = serializedSink;
			}
			return sink.onRequest(consumer);
		}

		@Override
		public FluxSink<T> onCancel(Disposable d) {
			sink.onCancel(d);
			return sink;
		}

		@Override
		public FluxSink<T> onDispose(Disposable d) {
			sink.onDispose(d);
			return this;
		}
	}

	static abstract class BaseSink<T> extends AtomicBoolean
			implements FluxSink<T>, InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BaseSink, Disposable> DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(BaseSink.class,
						Disposable.class,
						"disposable");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BaseSink> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BaseSink.class, "requested");

		volatile LongConsumer requestConsumer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BaseSink, LongConsumer>
				REQUEST_CONSUMER = AtomicReferenceFieldUpdater.newUpdater(BaseSink.class,
				LongConsumer.class,
				"requestConsumer");

		BaseSink(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
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
				disposeResource(false);
			}
		}

		@Override
		public void error(Throwable e) {
			if (isCancelled()) {
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			try {
				actual.onError(e);
			}
			finally {
				disposeResource(false);
			}
		}

		@Override
		public final void cancel() {
			disposeResource(true);
			onCancel();
		}

		void disposeResource(boolean isCancel) {
			Disposable d = disposable;
			if (d != OperatorDisposables.DISPOSED) {
				d = DISPOSABLE.getAndSet(this, OperatorDisposables.DISPOSED);
				if (d != null && d != OperatorDisposables.DISPOSED) {
					if (isCancel && d instanceof SinkDisposable) {
						((SinkDisposable) d).cancel();
					}
					d.dispose();
				}
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		void onCancel() {
			// default is no-op
		}

		@Override
		public final boolean isCancelled() {
			return OperatorDisposables.isDisposed(disposable);
		}

		@Override
		public final void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);

				LongConsumer consumer = requestConsumer;
				if (n > 0 && consumer != null && !isCancelled()) {
					consumer.accept(n);
				}
				onRequestedFromDownstream();
			}
		}

		void onRequestedFromDownstream() {
			// default is no-op
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public FluxSink<T> onRequest(LongConsumer consumer) {
			Objects.requireNonNull(consumer, "onRequest");
			onRequest(consumer, n -> {
			}, Long.MAX_VALUE);
			return this;
		}

		protected void onRequest(LongConsumer initialRequestConsumer,
				LongConsumer requestConsumer,
				long value) {
			if (!REQUEST_CONSUMER.compareAndSet(this, null, requestConsumer)) {
				throw new IllegalStateException(
						"A consumer has already been assigned to consume requests");
			}
			else if (value > 0) {
				initialRequestConsumer.accept(value);
			}
		}

		@Override
		public final FluxSink<T> onCancel(Disposable d) {
			Objects.requireNonNull(d, "onCancel");
			SinkDisposable sd = new SinkDisposable(null, d);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (c instanceof SinkDisposable) {
					SinkDisposable current = (SinkDisposable) c;
					if (current.onCancel == null) {
						current.onCancel = d;
					}
					else {
						d.dispose();
					}
				}
			}
			return this;
		}

		@Override
		public final FluxSink<T> onDispose(Disposable d) {
			Objects.requireNonNull(d, "onDispose");
			SinkDisposable sd = new SinkDisposable(d, null);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (c == OperatorDisposables.DISPOSED) {
					d.dispose();
				}
				else if (c instanceof SinkDisposable) {
					SinkDisposable current = (SinkDisposable) c;
					if (current.disposable == null) {
						current.disposable = d;
					}
					else {
						d.dispose();
					}
				}
			}
			return this;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) {
				return OperatorDisposables.isDisposed(disposable);
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}

			return InnerProducer.super.scanUnsafe(key);
		}
	}

	static final class IgnoreSink<T> extends BaseSink<T> {

		IgnoreSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public FluxSink<T> next(T t) {
			if (isCancelled()) {
				Operators.onNextDropped(t, actual.currentContext());
				return this;
			}

			actual.onNext(t);

			for (; ; ) {
				long r = requested;
				if (r == 0L || REQUESTED.compareAndSet(this, r, r - 1)) {
					return this;
				}
			}
		}

	}

	static abstract class NoOverflowBaseAsyncSink<T> extends BaseSink<T> {

		NoOverflowBaseAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public final FluxSink<T> next(T t) {
			if (isCancelled()) {
				Operators.onNextDropped(t, actual.currentContext());
				return this;
			}

			if (requested != 0) {
				actual.onNext(t);
				Operators.produced(REQUESTED, this, 1);
			}
			else {
				onOverflow();
			}
			return this;
		}

		abstract void onOverflow();
	}

	static final class DropAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		DropAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void onOverflow() {
			// nothing to do
		}

	}

	static final class ErrorAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		ErrorAsyncSink(CoreSubscriber<? super T> actual) {
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

		BufferAsyncSink(CoreSubscriber<? super T> actual, int capacityHint) {
			super(actual);
			this.queue = Queues.<T>unbounded(capacityHint).get();
		}

		@Override
		public FluxSink<T> next(T t) {
			queue.offer(t);
			drain();
			return this;
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

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return queue.size();
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.ERROR) {
				return error;
			}

			return super.scanUnsafe(key);
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

		LatestAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
			this.queue = new AtomicReference<>();
		}

		@Override
		public FluxSink<T> next(T t) {
			queue.set(t);
			drain();
			return this;
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

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return queue.get() == null ? 0 : 1;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.ERROR) {
				return error;
			}

			return super.scanUnsafe(key);
		}
	}

	static final class SinkDisposable implements Disposable {

		Disposable onCancel;

		Disposable disposable;

		SinkDisposable(@Nullable Disposable disposable, @Nullable Disposable onCancel) {
			this.disposable = disposable;
			this.onCancel = onCancel;
		}

		@Override
		public void dispose() {
			if (disposable != null) {
				disposable.dispose();
			}
		}

		public void cancel() {
			if (onCancel != null) {
				onCancel.dispose();
			}
		}
	}
}

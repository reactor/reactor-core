/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.FluxCreate.SinkDisposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Wraps the downstream Subscriber into a single emission object and calls the given
 * callback to produce a signal (a)synchronously.
 *
 * @param <T> the value type
 */
final class MonoCreate<T> extends Mono<T> implements SourceProducer<T> {

	static final Disposable TERMINATED = OperatorDisposables.DISPOSED;
	static final Disposable CANCELLED = Disposables.disposed();

	final Consumer<MonoSink<T>> callback;

	MonoCreate(Consumer<MonoSink<T>> callback) {
		this.callback = callback;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		DefaultMonoSink<T> emitter = new DefaultMonoSink<>(actual);

		actual.onSubscribe(emitter);

		try {
			callback.accept(emitter);
		}
		catch (Throwable ex) {
			emitter.error(Operators.onOperatorError(ex, actual.currentContext()));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
		return null;
	}

	static final class DefaultMonoSink<T> extends AtomicBoolean
			implements MonoSink<T>, InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultMonoSink, Disposable> DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class,
						Disposable.class,
						"disposable");

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DefaultMonoSink> STATE =
				AtomicIntegerFieldUpdater.newUpdater(DefaultMonoSink.class, "state");

		volatile LongConsumer requestConsumer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultMonoSink, LongConsumer>
				REQUEST_CONSUMER =
				AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class,
						LongConsumer.class,
						"requestConsumer");

		T value;

		static final int NO_REQUEST_HAS_VALUE  = 1;
		static final int HAS_REQUEST_NO_VALUE  = 2;
		static final int HAS_REQUEST_HAS_VALUE = 3;

		DefaultMonoSink(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public Context currentContext() {
			return this.actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) {
				return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE || disposable == TERMINATED;
			}
			if (key == Attr.CANCELLED) {
				return disposable == CANCELLED;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.ASYNC;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void success() {
			if (isDisposed()) {
				return;
			}
			if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
				try {
					actual.onComplete();
				}
				finally {
					disposeResource(false);
				}
			}
		}

		@Override
		public void success(@Nullable T value) {
			if (value == null) {
				success();
				return;
			}
			if (isDisposed()) {
				Operators.onNextDropped(value, actual.currentContext());
				return;
			}
			for (; ; ) {
				int s = state;
				if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
					Operators.onNextDropped(value, actual.currentContext());
					return;
				}
				if (s == HAS_REQUEST_NO_VALUE) {
					if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
						try {
							actual.onNext(value);
							actual.onComplete();
						}
						catch (Throwable t) {
							actual.onError(t);
						}
						finally {
							disposeResource(false);
						}
					} else {
						Operators.onNextDropped(value, actual.currentContext());
					}
					return;
				}
				this.value = value;
				if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
					return;
				}
			}
		}

		@Override
		public void error(Throwable e) {
			if (isDisposed()) {
				Operators.onOperatorError(e, actual.currentContext());
				return;
			}
			if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
				try {
					actual.onError(e);
				}
				finally {
					disposeResource(false);
				}
			}
			else {
				Operators.onOperatorError(e, actual.currentContext());
			}
		}

		@Override
		public MonoSink<T> onRequest(LongConsumer consumer) {
			Objects.requireNonNull(consumer, "onRequest");
			if (!REQUEST_CONSUMER.compareAndSet(this, null, consumer)) {
				throw new IllegalStateException(
						"A consumer has already been assigned to consume requests");
			}
			int s = this.state;
			if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
				consumer.accept(Long.MAX_VALUE);
			}
			return this;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public MonoSink<T> onCancel(Disposable d) {
			Objects.requireNonNull(d, "onCancel");
			SinkDisposable sd = new SinkDisposable(null, d);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (c == CANCELLED) {
					d.dispose();
				}
				else if (c instanceof SinkDisposable) {
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
		public MonoSink<T> onDispose(Disposable d) {
			Objects.requireNonNull(d, "onDispose");
			SinkDisposable sd = new SinkDisposable(d, null);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (isDisposed()) {
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
		public void request(long n) {
			if (Operators.validate(n)) {
				LongConsumer consumer = requestConsumer;
				if (consumer != null) {
					consumer.accept(n);
				}
				for (; ; ) {
					int s = state;
					if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
						return;
					}
					if (s == NO_REQUEST_HAS_VALUE) {
						if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
							try {
								actual.onNext(value);
								actual.onComplete();
							}
							finally {
								disposeResource(false);
							}
						}
						return;
					}
					if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
						return;
					}
				}
			}
		}

		@Override
		public void cancel() {
			if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
				T old = value;
				value = null;
				Operators.onDiscard(old, actual.currentContext());
				disposeResource(true);
			}
		}

		void disposeResource(boolean isCancel) {
			Disposable target = isCancel ? CANCELLED : TERMINATED;
			Disposable d = disposable;
			if (d != TERMINATED && d != CANCELLED) {
				d = DISPOSABLE.getAndSet(this, target);
				if (d != null && d != TERMINATED && d != CANCELLED) {
					if (isCancel && d instanceof SinkDisposable) {
						((SinkDisposable) d).cancel();
					}
					d.dispose();
				}
			}
		}

		@Override
		public String toString() {
			return "MonoSink";
		}

		boolean isDisposed() {
			Disposable d = disposable;
			return d == CANCELLED || d == TERMINATED;
		}

	}
}

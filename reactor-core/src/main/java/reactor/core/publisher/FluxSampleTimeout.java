/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Emits the last value from upstream only if there were no newer values emitted
 * during the time window provided by a publisher for that particular last value.
 *
 * @param <T> the source value type
 * @param <U> the value type of the duration publisher
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSampleTimeout<T, U> extends InternalFluxOperator<T, T> {

	final Function<? super T, ? extends Publisher<U>> throttler;

	final Supplier<Queue<Object>> queueSupplier;

	FluxSampleTimeout(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<U>> throttler,
			Supplier<Queue<Object>> queueSupplier) {
		super(source);
		this.throttler = Objects.requireNonNull(throttler, "throttler");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Queue<SampleTimeoutOther<T, U>> q = (Queue) queueSupplier.get();

		SampleTimeoutMain<T, U> main = new SampleTimeoutMain<>(actual, throttler, q);

		actual.onSubscribe(main);

		return main;
	}

	static final class SampleTimeoutMain<T, U>implements InnerOperator<T, T> {

		final Function<? super T, ? extends Publisher<U>> throttler;
		final Queue<SampleTimeoutOther<T, U>>             queue;
		final CoreSubscriber<? super T>                   actual;
		final Context                                     ctx;

		volatile Subscription s;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleTimeoutMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SampleTimeoutMain.class,
						Subscription.class,
						"s");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleTimeoutMain, Subscription>
				OTHER = AtomicReferenceFieldUpdater.newUpdater(SampleTimeoutMain.class,
				Subscription.class,
				"other");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SampleTimeoutMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SampleTimeoutMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SampleTimeoutMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SampleTimeoutMain.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SampleTimeoutMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SampleTimeoutMain.class,
						Throwable.class,
						"error");

		volatile boolean done;

		volatile boolean cancelled;

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SampleTimeoutMain> INDEX =
				AtomicLongFieldUpdater.newUpdater(SampleTimeoutMain.class, "index");

		SampleTimeoutMain(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<U>> throttler,
				Queue<SampleTimeoutOther<T, U>> queue) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.throttler = throttler;
			this.queue = queue;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(other));
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PARENT) return s;
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.BUFFERED) return queue.size();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				Operators.terminate(S, this);
				Operators.terminate(OTHER, this);
				Operators.onDiscardQueueWithClear(queue, ctx, SampleTimeoutOther::toStream);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			long idx = INDEX.incrementAndGet(this);

			if (!Operators.set(OTHER, this, Operators.emptySubscription())) {
				return;
			}

			Publisher<U> p;

			try {
				p = Objects.requireNonNull(throttler.apply(t),
						"throttler returned a null publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, ctx));
				return;
			}

			SampleTimeoutOther<T, U> os = new SampleTimeoutOther<>(this, t, idx);

			if (Operators.replace(OTHER, this, os)) {
				p.subscribe(os);
			}
		}

		void error(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t, ctx);
			}
		}

		@Override
		public void onError(Throwable t) {
			Operators.terminate(OTHER, this);

			error(t);
		}

		@Override
		public void onComplete() {
			Subscription o = other;
			if (o instanceof FluxSampleTimeout.SampleTimeoutOther) {
				SampleTimeoutOther<?, ?> os = (SampleTimeoutOther<?, ?>) o;
				os.cancel();
				os.onComplete();
			}
			done = true;
			drain();
		}

		void otherNext(SampleTimeoutOther<T, U> other) {
			queue.offer(other);
			drain();
		}

		void otherError(long idx, Throwable e) {
			if (idx == index) {
				Operators.terminate(S, this);

				error(e);
			}
			else {
				Operators.onErrorDropped(e, ctx);
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super T> a = actual;
			final Queue<SampleTimeoutOther<T, U>> q = queue;

			int missed = 1;

			for (; ; ) {

				for (; ; ) {
					boolean d = done;

					SampleTimeoutOther<T, U> o = q.poll();

					boolean empty = o == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					if (o.index == index) {
						long r = requested;
						if (r != 0) {
							a.onNext(o.value);
							if (r != Long.MAX_VALUE) {
								REQUESTED.decrementAndGet(this);
							}
						}
						else {
							cancel();

							Operators.onDiscardQueueWithClear(q, ctx, SampleTimeoutOther::toStream);

							Throwable e = Exceptions.failWithOverflow(
									"Could not emit value due to lack of requests");
							Exceptions.addThrowable(ERROR, this, e);
							e = Exceptions.terminate(ERROR, this);

							a.onError(e);
							return;
						}
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<SampleTimeoutOther<T, U>> q) {
			if (cancelled) {
				Operators.onDiscardQueueWithClear(q, ctx, SampleTimeoutOther::toStream);
				return true;
			}
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancel();

					Operators.onDiscardQueueWithClear(q, ctx, SampleTimeoutOther::toStream);

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
	}

	static final class SampleTimeoutOther<T, U> extends Operators.DeferredSubscription
			implements InnerConsumer<U> {

		final SampleTimeoutMain<T, U> main;

		final T value;

		final long index;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SampleTimeoutOther> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(SampleTimeoutOther.class, "once");

		SampleTimeoutOther(SampleTimeoutMain<T, U> main, T value, long index) {
			this.main = main;
			this.value = value;
			this.index = index;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return once == 1;
			if (key == Attr.ACTUAL) return main;

			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				cancel();

				main.otherNext(this);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				main.otherError(index, t);
			}
			else {
				Operators.onErrorDropped(t, main.currentContext());
			}
		}

		@Override
		public void onComplete() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				main.otherNext(this);
			}
		}

		final Stream<T> toStream() {
			return Stream.of(value);
		}

	}
}

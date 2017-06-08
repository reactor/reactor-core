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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * buffers elements into possibly overlapping buffers whose boundaries are determined
 * by a start Publisher's element and a signal of a derived Publisher
 *
 * @param <T> the source value type
 * @param <U> the value type of the publisher opening the buffers
 * @param <V> the value type of the publisher closing the individual buffers
 * @param <C> the collection type that holds the buffered values
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBufferWhen<T, U, V, C extends Collection<? super T>>
		extends FluxOperator<T, C> {

	final Publisher<U> start;

	final Function<? super U, ? extends Publisher<V>> end;

	final Supplier<C> bufferSupplier;

	final Supplier<? extends Queue<C>> queueSupplier;

	FluxBufferWhen(Flux<? extends T> source,
			Publisher<U> start,
			Function<? super U, ? extends Publisher<V>> end,
			Supplier<C> bufferSupplier,
			Supplier<? extends Queue<C>> queueSupplier) {
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
	public void subscribe(Subscriber<? super C> s, Context ctx) {

		Queue<C> q = queueSupplier.get();

		BufferStartEndMainSubscriber<T, U, V, C> parent =
				new BufferStartEndMainSubscriber<>(s, bufferSupplier, q, end);

		s.onSubscribe(parent);

		start.subscribe(parent.starter);

		source.subscribe(parent, ctx);
	}

	static final class BufferStartEndMainSubscriber<T, U, V, C extends Collection<? super T>>
			extends CachedContextProducer<C>
			implements InnerOperator<T, C> {
		final Supplier<C> bufferSupplier;

		final Queue<C> queue;

		final Function<? super U, ? extends Publisher<V>> end;

		Set<Subscription> endSubscriptions;

		final BufferStartEndStarter<U> starter;

		Map<Long, C> buffers;

		volatile Subscription s;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BufferStartEndMainSubscriber, Subscription>
				S =
				AtomicReferenceFieldUpdater.newUpdater(BufferStartEndMainSubscriber.class,
						Subscription.class,
						"s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferStartEndMainSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferStartEndMainSubscriber.class,
						"requested");

		long index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferStartEndMainSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BufferStartEndMainSubscriber.class,
						"wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BufferStartEndMainSubscriber, Throwable>
				ERROR = AtomicReferenceFieldUpdater.newUpdater(
				BufferStartEndMainSubscriber.class,
				Throwable.class,
				"error");

		volatile boolean done;

		volatile boolean cancelled;

		volatile int open;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferStartEndMainSubscriber> OPEN =
				AtomicIntegerFieldUpdater.newUpdater(BufferStartEndMainSubscriber.class,
						"open");

		BufferStartEndMainSubscriber(Subscriber<? super C> actual,
				Supplier<C> bufferSupplier,
				Queue<C> queue,
				Function<? super U, ? extends Publisher<V>> end) {
			super(actual);
			this.bufferSupplier = bufferSupplier;
			this.buffers = new HashMap<>();
			this.endSubscriptions = new HashSet<>();
			this.queue = queue;
			this.end = end;
			this.open = 1;
			this.starter = new BufferStartEndStarter<>(this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set != null) {
					for (C b : set.values()) {
						b.add(t);
					}
					return;
				}
			}

			Operators.onNextDropped(t);
		}

		@Override
		public void onError(Throwable t) {
			boolean report;
			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set != null) {
					buffers = null;
					report = true;
				}
				else {
					report = false;
				}
			}

			if (report) {
				anyError(t);
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			Map<Long, C> set;

			synchronized (this) {
				set = buffers;
				if (set == null) {
					return;
				}
			}

			cancelStart();
			cancelEnds();

			for (C b : set.values()) {
				queue.offer(b);
			}
			done = true;
			drain();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		void cancelMain() {
			Operators.terminate(S, this);
		}

		void cancelStart() {
			starter.cancel();
		}

		void cancelEnds() {
			Set<Subscription> set;
			synchronized (starter) {
				set = endSubscriptions;

				if (set == null) {
					return;
				}
				endSubscriptions = null;
			}

			for (Subscription s : set) {
				s.cancel();
			}
		}

		boolean addEndSubscription(Subscription s) {
			synchronized (starter) {
				Set<Subscription> set = endSubscriptions;

				if (set != null) {
					set.add(s);
					return true;
				}
			}
			s.cancel();
			return false;
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				cancelMain();

				cancelStart();

				cancelEnds();
			}
		}

		boolean emit(C b) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(b);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			}
			else {

				actual.onError(Exceptions.failWithOverflow(
						"Could not emit buffer due to lack of requests"));

				return false;
			}
		}

		void anyError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		void startNext(U u) {

			long idx = index;
			index = idx + 1;

			C b;

			try {
				b = Objects.requireNonNull(bufferSupplier.get(),
				"The bufferSupplier returned a null buffer");
			}
			catch (Throwable e) {
				anyError(Operators.onOperatorError(starter, e, u));
				return;
			}

			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set == null) {
					return;
				}

				set.put(idx, b);
			}

			Publisher<V> p;

			try {
				p = Objects.requireNonNull(end.apply(u),
				"The end returned a null publisher");
			}
			catch (Throwable e) {
				anyError(Operators.onOperatorError(starter, e, u));
				return;
			}

			BufferStartEndEnder<T, V, C> end = new BufferStartEndEnder<>(this, b, idx);

			if (addEndSubscription(end)) {
				OPEN.getAndIncrement(this);

				p.subscribe(end);
			}
		}

		void startError(Throwable e) {
			anyError(e);
		}

		void startComplete() {
			if (OPEN.decrementAndGet(this) == 0) {
				cancelAll();
				done = true;
				drain();
			}
		}

		void cancelAll() {
			cancelMain();

			cancelStart();

			cancelEnds();
		}

		void endSignal(BufferStartEndEnder<T, V, C> ender) {
			synchronized (this) {
				Map<Long, C> set = buffers;

				if (set == null) {
					return;
				}

				if (set.remove(ender.index) == null) {
					return;
				}

				queue.offer(ender.buffer);
			}
			if (OPEN.decrementAndGet(this) == 0) {
				cancelAll();
				done = true;
			}
			drain();
		}

		void endError(Throwable e) {
			anyError(e);
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super C> a = actual;
			final Queue<C> q = queue;

			int missed = 1;

			for (; ; ) {

				for (; ; ) {
					boolean d = done;

					C b = q.poll();

					boolean empty = b == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					long r = requested;
					if (r != 0L) {
						actual.onNext(b);
						if (r != Long.MAX_VALUE) {
							REQUESTED.decrementAndGet(this);
						}
					}
					else {
						anyError(Exceptions.failWithOverflow(
								"Could not emit buffer due to lack of requests"));
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				queue.clear();
				return true;
			}

			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancel();
					queue.clear();
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
			if (key == IntAttr.BUFFERED) return buffers.values()
			                                           .stream()
			                                           .mapToInt(Collection::size)
			                                           .sum();
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class BufferStartEndStarter<U> extends Operators.DeferredSubscription
			implements InnerConsumer<U> {

		final BufferStartEndMainSubscriber<?, U, ?, ?> main;

		BufferStartEndStarter(BufferStartEndMainSubscriber<?, U, ?, ?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			main.startNext(t);
		}

		@Override
		public void onError(Throwable t) {
			main.startError(t);
		}

		@Override
		public void onComplete() {
			main.startComplete();
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.ACTUAL) {
				return main;
			}
			return super.scanUnsafe(key);
		}
	}

	static final class BufferStartEndEnder<T, V, C extends Collection<? super T>>
			extends Operators.DeferredSubscription implements InnerConsumer<V> {

		final BufferStartEndMainSubscriber<T, ?, V, C> main;

		final C buffer;

		final long index;

		BufferStartEndEnder(BufferStartEndMainSubscriber<T, ?, V, C> main,
				C buffer,
				long index) {
			this.main = main;
			this.buffer = buffer;
			this.index = index;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.ACTUAL) {
				return main;
			}
			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(V t) {
			if (!isCancelled()) {
				cancel();

				main.endSignal(this);
			}
		}

		@Override
		public void onError(Throwable t) {
			main.endError(t);
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				main.endSignal(this);
			}
		}

	}
}

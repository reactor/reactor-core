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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;


/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an
 * item.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSwitchMap<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final Supplier<? extends Queue<Object>> queueSupplier;

	final int bufferSize;

	static final SwitchMapInner<Object> CANCELLED_INNER =
			new SwitchMapInner<>(null, 0, Long.MAX_VALUE);

	FluxSwitchMap(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			Supplier<? extends Queue<Object>> queueSupplier,
			int bufferSize) {
		super(source);
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.bufferSize = bufferSize;
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper, false)) {
			return;
		}

		source.subscribe(new SwitchMapMain<T, R>(s,
				mapper,
				queueSupplier.get(),
				bufferSize));
	}

	static final class SwitchMapMain<T, R>
			implements InnerOperator<T, R> {

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final Queue<Object> queue;

		final BiPredicate<Object, Object> queueBiAtomic;

		final int                   bufferSize;
		final Subscriber<? super R> actual;

		Subscription s;

		volatile boolean done;

		volatile Throwable error;

		@Override
		public final Subscriber<? super R> actual() {
			return actual;
		}

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapMain.class,
						Throwable.class,
						"error");

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "once");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "wip");

		volatile SwitchMapInner<R> inner;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapMain, SwitchMapInner> INNER =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapMain.class,
						SwitchMapInner.class,
						"inner");

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> INDEX =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "index");

		volatile int active;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "active");

		@SuppressWarnings("unchecked")
		SwitchMapMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				Queue<Object> queue,
				int bufferSize) {
			this.actual = actual;
			this.mapper = mapper;
			this.queue = queue;
			this.bufferSize = bufferSize;
			this.active = 1;
			if(queue instanceof BiPredicate){
				this.queueBiAtomic = (BiPredicate<Object, Object>) queue;
			}
			else {
				this.queueBiAtomic = null;
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == IntAttr.PREFETCH) return bufferSize;
			if (key == IntAttr.BUFFERED) return queue.size();
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(inner);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			long idx = INDEX.incrementAndGet(this);

			SwitchMapInner<R> si = inner;
			if (si != null) {
				si.deactivate();
				si.cancel();
			}

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
				"The mapper returned a null publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			SwitchMapInner<R> innerSubscriber =
					new SwitchMapInner<>(this, bufferSize, idx);

			if (INNER.compareAndSet(this, si, innerSubscriber)) {
				ACTIVE.getAndIncrement(this);
				p.subscribe(innerSubscriber);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}

			if (Exceptions.addThrowable(ERROR, this, t)) {

				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}

				cancelInner();
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			if (ONCE.compareAndSet(this, 0, 1)) {
				deactivate();
			}

			done = true;
			drain();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				if (WIP.getAndIncrement(this) == 0) {
					cancelAndCleanup(queue);
				}
			}
		}

		void deactivate() {
			ACTIVE.decrementAndGet(this);
		}

		void cancelInner() {
			SwitchMapInner<?> si = INNER.getAndSet(this, CANCELLED_INNER);
			if (si != null && si != CANCELLED_INNER) {
				si.cancel();
				si.deactivate();
			}
		}

		void cancelAndCleanup(Queue<?> q) {
			s.cancel();

			cancelInner();

			q.clear();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			Subscriber<? super R> a = actual;
			Queue<Object> q = queue;

			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = active == 0;

					@SuppressWarnings("unchecked") SwitchMapInner<R> si =
							(SwitchMapInner<R>) q.poll();

					boolean empty = si == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					Object second;

					while ((second = q.poll()) == null) {
					}

					if (index == si.index) {

						@SuppressWarnings("unchecked") R v = (R) second;

						a.onNext(v);

						si.requestOne();

						e++;
					}
				}

				if (r == e) {
					if (checkTerminated(active == 0, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				cancelAndCleanup(q);
				return true;
			}

			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancelAndCleanup(q);

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

		void innerNext(SwitchMapInner<R> inner, R value) {
			queueBiAtomic.test(inner, value);
			drain();
		}

		void innerError(SwitchMapInner<R> inner, Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				s.cancel();

				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}
				inner.deactivate();
				drain();
			}
			else {
				Operators.onErrorDropped(e);
			}
		}

		void innerComplete(SwitchMapInner<R> inner) {
			inner.deactivate();
			drain();
		}
	}

	static final class SwitchMapInner<R> implements InnerConsumer<R>, Subscription {

		final SwitchMapMain<?, R> parent;

		final int bufferSize;

		final int limit;

		final long index;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapInner.class, "once");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapInner.class,
						Subscription.class,
						"s");

		int produced;

		SwitchMapInner(SwitchMapMain<?, R> parent, int bufferSize, long index) {
			this.parent = parent;
			this.bufferSize = bufferSize;
			this.limit = bufferSize - (bufferSize >> 2);
			this.index = index;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == IntAttr.PREFETCH) return bufferSize;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			Subscription a = this.s;
			if (a == Operators.cancelledSubscription()) {
				s.cancel();
			}
			if (a != null) {
				s.cancel();

				Operators.reportSubscriptionSet();
				return;
			}

			if (S.compareAndSet(this, null, s)) {
				s.request(bufferSize);
				return;
			}
			a = this.s;
			if (a != Operators.cancelledSubscription()) {
				s.cancel();

				Operators.reportSubscriptionSet();
			}
		}

		@Override
		public void onNext(R t) {
			parent.innerNext(this, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(this, t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(this);
		}

		void deactivate() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				parent.deactivate();
			}
		}

		void requestOne() {
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			}
			else {
				produced = p;
			}
		}

		@Override
		public void request(long n) {
			long p = produced + n;
			if (p >= limit) {
				produced = 0;
				s.request(p);
			}
			else {
				produced = (int) p;
			}
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
		}
	}
}

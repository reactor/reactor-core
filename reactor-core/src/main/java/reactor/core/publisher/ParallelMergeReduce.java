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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Reduces all 'rails' into a single value which then gets reduced into a single
 * Publisher sequence.
 *
 * @param <T> the value type
 */
final class ParallelMergeReduce<T> extends Mono<T> implements Scannable, Fuseable {

	final ParallelFlux<? extends T> source;

	final BiFunction<T, T, T> reducer;

	ParallelMergeReduce(ParallelFlux<? extends T> source,
			BiFunction<T, T, T> reducer) {
		this.source = source;
		this.reducer = reducer;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MergeReduceMain<T> parent =
				new MergeReduceMain<>(actual, source.parallelism(), reducer);
		actual.onSubscribe(parent);

		source.subscribe(parent.subscribers);
	}

	static final class MergeReduceMain<T>
			extends Operators.MonoSubscriber<T, T> {

		final MergeReduceInner<T>[] subscribers;

		final BiFunction<T, T, T> reducer;

		volatile SlotPair<T> current;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceMain, SlotPair>
				CURRENT = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceMain.class,
				SlotPair.class,
				"current");

		volatile int remaining;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeReduceMain>
				REMAINING = AtomicIntegerFieldUpdater.newUpdater(
				MergeReduceMain.class,
				"remaining");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceMain, Throwable>
				ERROR = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceMain.class,
				Throwable.class,
				"error");

		MergeReduceMain(CoreSubscriber<? super T> subscriber,
				int n,
				BiFunction<T, T, T> reducer) {
			super(subscriber);
			@SuppressWarnings("unchecked") MergeReduceInner<T>[] a =
					new MergeReduceInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new MergeReduceInner<>(this, reducer);
			}
			this.subscribers = a;
			this.reducer = reducer;
			REMAINING.lazySet(this, n);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return REMAINING.get(this) == 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Nullable
		SlotPair<T> addValue(T value) {
			for (; ; ) {
				SlotPair<T> curr = current;

				if (curr == null) {
					curr = new SlotPair<>();
					if (!CURRENT.compareAndSet(this, null, curr)) {
						continue;
					}
				}

				int c = curr.tryAcquireSlot();
				if (c < 0) {
					CURRENT.compareAndSet(this, curr, null);
					continue;
				}
				if (c == 0) {
					curr.first = value;
				}
				else {
					curr.second = value;
				}

				if (curr.releaseSlot()) {
					CURRENT.compareAndSet(this, curr, null);
					return curr;
				}
				return null;
			}
		}

		@Override
		public void cancel() {
			for (MergeReduceInner<T> inner : subscribers) {
				inner.cancel();
			}
			super.cancel();
		}

		void innerError(Throwable ex) {
			if(ERROR.compareAndSet(this, null, ex)){
				cancel();
				actual.onError(ex);
			}
			else if(error != ex) {
				Operators.onErrorDropped(ex, actual.currentContext());
			}
		}

		void innerComplete(@Nullable T value) {
			if (value != null) {
				for (; ; ) {
					SlotPair<T> sp = addValue(value);

					if (sp != null) {

						try {
							value = Objects.requireNonNull(reducer.apply(sp.first,
									sp.second), "The reducer returned a null value");
						}
						catch (Throwable ex) {
							innerError(Operators.onOperatorError(this, ex,
									actual.currentContext()));
							return;
						}
					}
					else {
						break;
					}
				}
			}

			if (REMAINING.decrementAndGet(this) == 0) {
				SlotPair<T> sp = current;
				CURRENT.lazySet(this, null);

				if (sp != null) {
					complete(sp.first);
				}
				else {
					actual.onComplete();
				}
			}
		}
	}

	static final class MergeReduceInner<T> implements InnerConsumer<T> {

		final MergeReduceMain<T> parent;

		final BiFunction<T, T, T> reducer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceInner, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceInner.class,
				Subscription.class,
				"s");

		T value;

		boolean done;

		MergeReduceInner(MergeReduceMain<T> parent,
				BiFunction<T, T, T> reducer) {
			this.parent = parent;
			this.reducer = reducer;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, currentContext());
				return;
			}
			T v = value;

			if (v == null) {
				value = t;
			}
			else {

				try {
					v = Objects.requireNonNull(reducer.apply(v, t), "The reducer returned a null value");
				}
				catch (Throwable ex) {
					onError(Operators.onOperatorError(s, ex, t, currentContext()));
					return;
				}

				value = v;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}
			done = true;
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			parent.innerComplete(value);
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}

	static final class SlotPair<T> {

		T first;

		T second;

		volatile int acquireIndex;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SlotPair> ACQ =
				AtomicIntegerFieldUpdater.newUpdater(SlotPair.class, "acquireIndex");

		volatile int releaseIndex;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SlotPair> REL =
				AtomicIntegerFieldUpdater.newUpdater(SlotPair.class, "releaseIndex");

		int tryAcquireSlot() {
			for (; ; ) {
				int acquired = acquireIndex;
				if (acquired >= 2) {
					return -1;
				}

				if (ACQ.compareAndSet(this, acquired, acquired + 1)) {
					return acquired;
				}
			}
		}

		boolean releaseSlot() {
			return REL.incrementAndGet(this) == 2;
		}
	}
}

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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;

/**
 * Reduces all 'rails' into a single value which then gets reduced into a single
 * Publisher sequence.
 *
 * @param <T> the value type
 */
final class ParallelMergeReduce<T> extends Mono<T> implements Fuseable {

	final ParallelFlux<? extends T> source;

	final BiFunction<T, T, T> reducer;

	ParallelMergeReduce(ParallelFlux<? extends T> source,
			BiFunction<T, T, T> reducer) {
		this.source = source;
		this.reducer = reducer;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		ParallelMergeReduceSubscriber<T> parent =
				new ParallelMergeReduceSubscriber<>(s, source.parallelism(), reducer);
		s.onSubscribe(parent);

		source.subscribe(parent.subscribers);
	}

	static final class ParallelMergeReduceSubscriber<T>
			extends Operators.MonoSubscriber<T, T> {

		final MergeReduceInnerSubscriber<T>[] subscribers;

		final BiFunction<T, T, T> reducer;

		volatile SlotPair<T> current;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ParallelMergeReduceSubscriber, SlotPair>
				CURRENT = AtomicReferenceFieldUpdater.newUpdater(
				ParallelMergeReduceSubscriber.class,
				SlotPair.class,
				"current");

		volatile int remaining;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelMergeReduceSubscriber>
				REMAINING = AtomicIntegerFieldUpdater.newUpdater(
				ParallelMergeReduceSubscriber.class,
				"remaining");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ParallelMergeReduceSubscriber, Throwable>
				ERROR = AtomicReferenceFieldUpdater.newUpdater(
				ParallelMergeReduceSubscriber.class,
				Throwable.class,
				"error");

		ParallelMergeReduceSubscriber(Subscriber<? super T> subscriber,
				int n,
				BiFunction<T, T, T> reducer) {
			super(subscriber);
			@SuppressWarnings("unchecked") MergeReduceInnerSubscriber<T>[] a =
					new MergeReduceInnerSubscriber[n];
			for (int i = 0; i < n; i++) {
				a[i] = new MergeReduceInnerSubscriber<>(this, reducer);
			}
			this.subscribers = a;
			this.reducer = reducer;
			REMAINING.lazySet(this, n);
		}

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
			for (MergeReduceInnerSubscriber<T> inner : subscribers) {
				inner.cancel();
			}
		}

		void innerError(Throwable ex) {
			if(ERROR.compareAndSet(this, null, ex)){
				cancel();
				actual.onError(ex);
			}
			else if(error != ex) {
				Operators.onErrorDropped(ex);
			}
		}

		void innerComplete(T value) {
			if (value != null) {
				for (; ; ) {
					SlotPair<T> sp = addValue(value);

					if (sp != null) {

						try {
							value = Objects.requireNonNull(reducer.apply(sp.first,
									sp.second), "The reducer returned a null value");
						}
						catch (Throwable ex) {
							innerError(Operators.onOperatorError(this, ex));
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

	static final class MergeReduceInnerSubscriber<T> implements Subscriber<T> {

		final ParallelMergeReduceSubscriber<T> parent;

		final BiFunction<T, T, T> reducer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceInnerSubscriber, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceInnerSubscriber.class,
				Subscription.class,
				"s");

		T value;

		boolean done;

		public MergeReduceInnerSubscriber(ParallelMergeReduceSubscriber<T> parent,
				BiFunction<T, T, T> reducer) {
			this.parent = parent;
			this.reducer = reducer;
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
				Operators.onNextDropped(t);
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
					onError(Operators.onOperatorError(s, ex, t));
					return;
				}

				value = v;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
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

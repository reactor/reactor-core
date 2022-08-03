/*
 * Copyright (c) 2019-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Observe termination from all 'rails' into a single Mono.
 */
final class ParallelThen extends Mono<Void> implements Scannable, Fuseable {

	final ParallelFlux<?> source;

	ParallelThen(ParallelFlux<?> source) {
		this.source = source;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> actual) {
		ThenMain parent = new ThenMain(actual, source);
		actual.onSubscribe(parent);
	}

	static final class ThenMain implements InnerProducer<Void>,
	                                       Fuseable, //for constants only
			                               QueueSubscription<Void> {

		final ThenInner[] subscribers;
		final CoreSubscriber<? super Void> actual;
		final ParallelFlux<?> source;

		volatile long state;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ThenMain> STATE =
				AtomicLongFieldUpdater.newUpdater(ThenMain.class, "state");


		static final long CANCELLED_FLAG =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long REQUESTED_FLAG =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long INNER_COMPLETED_MAX =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0111_1111_1111_1111_1111_1111_1111_1111L;

		ThenMain(CoreSubscriber<? super Void> actual, final ParallelFlux<?> source) {
			this.actual = actual;
			this.source = source;

			final int n = source.parallelism();
			ThenInner[] a = new ThenInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new ThenInner(this);
			}
			this.subscribers = a;
		}

		@Override
		public CoreSubscriber<? super Void> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return innersCompletedCount(this.state) == source.parallelism();
			if (key == Attr.CANCELLED) return isCancelled(this.state) && innersCompletedCount(this.state) != source.parallelism();
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			final long previousState = markCancelled(this);

			if (isCancelled(previousState) || !isRequestedOnce(previousState)) {
				return;
			}

			for (ThenInner inner : subscribers) {
				inner.cancel();
			}
		}

		@Override
		public void request(long n) {
			if (!STATE.compareAndSet(this, 0, REQUESTED_FLAG)) {
				return;
			}

			source.subscribe(this.subscribers);
		}

		void innerError(Throwable ex, ThenInner innerCaller) {
			final long previousState = markForceTerminated(this);

			final int n = this.source.parallelism();

			if (isCancelled(previousState) || innersCompletedCount(previousState) == n) {
				return;
			}

			for (ThenInner inner : subscribers) {
				if (inner != innerCaller) {
					inner.cancel();
				}
			}

			actual.onError(ex);
		}

		void innerComplete() {
			final long previousState = markInnerCompleted(this);

			final int n = this.source.parallelism();
			final int innersCompletedCount = innersCompletedCount(previousState);

			if (isCancelled(previousState) || innersCompletedCount == n) {
				return;
			}

			if ((innersCompletedCount + 1) == n) {
				actual.onComplete();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public Void poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}

		static long markForceTerminated(ThenMain instance) {
			final int n = instance.source.parallelism();
			for (;;) {
				final long state = instance.state;

				if (isCancelled(state) || innersCompletedCount(state) == n) {
					return state;
				}

				final long nextState = (state & ~INNER_COMPLETED_MAX) | CANCELLED_FLAG | n;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean isRequestedOnce(long state) {
			return (state & REQUESTED_FLAG) == REQUESTED_FLAG;
		}

		static long markCancelled(ThenMain instance) {
			final int n = instance.source.parallelism();
			for(;;) {
				final long state = instance.state;

				if (isCancelled(state) || innersCompletedCount(state) == n) {
					return state;
				}

				final long nextState = state | CANCELLED_FLAG;
				if (STATE.weakCompareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static int innersCompletedCount(long state) {
			return (int) (state & INNER_COMPLETED_MAX);
		}

		static long markInnerCompleted(ThenMain instance) {
			final int n = instance.source.parallelism();
			for (;;) {
				final long state = instance.state;

				if (isCancelled(state) || innersCompletedCount(state) == n) {
					return state;
				}

				final long nextState = state + 1;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

	}

	static final class ThenInner implements InnerConsumer<Object> {

		final ThenMain parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThenInner, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(
				ThenInner.class,
				Subscription.class,
				"s");

		ThenInner(ThenMain parent) {
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
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
		public void onNext(Object t) {
			//ignored
			Operators.onDiscard(t, parent.actual.currentContext());
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t, this);
		}

		@Override
		public void onComplete() {
			parent.innerComplete();
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}

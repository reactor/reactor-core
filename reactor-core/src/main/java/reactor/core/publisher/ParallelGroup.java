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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Exposes the 'rails' as individual GroupedFlux instances, keyed by the rail index (zero based).
 * <p>
 * Each group can be consumed only once; requests and cancellation compose through. Note
 * that cancelling only one rail may result in undefined behavior.
 *
 * @param <T> the value type
 */
final class ParallelGroup<T> extends Flux<GroupedFlux<Integer, T>> implements
                                                                   Scannable, Fuseable {

	final ParallelFlux<? extends T> source;

	ParallelGroup(ParallelFlux<? extends T> source) {
		this.source = source;
	}

	@Override
	public void subscribe(CoreSubscriber<? super GroupedFlux<Integer, T>> actual) {
		int n = source.parallelism();

		@SuppressWarnings("unchecked")
		ParallelInnerGroup<T>[] groups = new ParallelInnerGroup[n];

		for (int i = 0; i < n; i++) {
			groups[i] = new ParallelInnerGroup<>(i);
		}

		FluxArray.subscribe(actual, groups);

		source.subscribe(groups);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	static final class ParallelInnerGroup<T> extends GroupedFlux<Integer, T>
	implements InnerOperator<T, T> {
		final int key;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelInnerGroup> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ParallelInnerGroup.class, "once");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ParallelInnerGroup, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ParallelInnerGroup.class, Subscription.class, "s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ParallelInnerGroup> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ParallelInnerGroup.class, "requested");

		CoreSubscriber<? super T> actual;

		ParallelInnerGroup(int key) {
			this.key = key;
		}

		@Override
		public Integer key() {
			return key;
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> actual) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				this.actual = actual;
				actual.onSubscribe(this);
			} else {
				Operators.error(actual, new IllegalStateException("This ParallelGroup can be subscribed to at most once."));
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					s.request(r);
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Subscription a = s;
				if (a == null) {
					Operators.addCap(REQUESTED, this, n);

					a = s;
					if (a != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							a.request(n);
						}
					}
				} else {
					a.request(n);
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}
	}
}

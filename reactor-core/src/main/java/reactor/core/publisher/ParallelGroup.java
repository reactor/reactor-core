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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;
import javax.annotation.Nullable;

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
	public void subscribe(Subscriber<? super GroupedFlux<Integer, T>> s, Context ctx) {
		int n = source.parallelism();

		@SuppressWarnings("unchecked")
		ParallelInnerGroup<T>[] groups = new ParallelInnerGroup[n];

		for (int i = 0; i < n; i++) {
			groups[i] = new ParallelInnerGroup<>(i, ctx);
		}

		FluxArray.subscribe(s, groups);

		source.subscribe(groups, ctx);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}

	static final class ParallelInnerGroup<T> extends GroupedFlux<Integer, T>
	implements InnerOperator<T, T> {
		final int key;

		final Context ctx;

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

		Subscriber<? super T> actual;

		ParallelInnerGroup(int key, Context ctx) {
			this.ctx = ctx;
			this.key = key;
		}

		@Override
		public Integer key() {
			return key;
		}

		@Override
		public void subscribe(Subscriber<? super T> s, Context context) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				this.actual = s;
				ContextRelay.set(s, ctx);
				s.onSubscribe(this);
			} else {
				Operators.error(s, new IllegalStateException("This ParallelGroup can be subscribed to at most once."));
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();

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
					Operators.getAndAddCap(REQUESTED, this, n);

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

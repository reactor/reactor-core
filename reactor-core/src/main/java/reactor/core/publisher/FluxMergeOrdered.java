/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * Merges the provided sources {@link org.reactivestreams.Publisher}, assuming a total order
 * of the values, by picking the smallest available value from each publisher, resulting in
 * a single totally ordered {@link Flux} sequence. This operator considers its primary
 * parent to be the first of the sources, for the purpose of {@link reactor.core.Scannable.Attr#PARENT}.
 *
 * @param <T> the value type
 * @author David Karnok
 * @author Simon Baslé
 */
//source: https://akarnokd.blogspot.fr/2017/09/java-9-flow-api-ordered-merge.html
final class FluxMergeOrdered<T> extends Flux<T> implements SourceProducer<T> {

	final int                      prefetch;
	final Supplier<Queue<T>>       queueSupplier;
	final Comparator<? super T>    valueComparator;
	final Publisher<? extends T>[] sources;

	@SafeVarargs
	FluxMergeOrdered(int prefetch,
			Supplier<Queue<T>> queueSupplier,
			Comparator<? super T> valueComparator,
			Publisher<? extends T>... sources) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.sources = Objects.requireNonNull(sources, "sources must be non-null");

		for (int i = 0; i < sources.length; i++) {
			Publisher<? extends T> source = sources[i];
			if (source == null) {
				throw new NullPointerException("sources[" + i + "] is null");
			}
		}

		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
		this.valueComparator = valueComparator;
	}

	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources. The provided {@link Comparator} is tested for equality
	 * with the current comparator, and if different is combined by {@link Comparator#thenComparing(Comparator)}.
	 * <p>
	 * This operation doesn't change the current {@link FluxMergeOrdered} instance.
	 *
	 * @param source the new source to merge with the others
	 * @return the new {@link FluxMergeOrdered} instance
	 */
	FluxMergeOrdered<T> mergeAdditionalSource(Publisher<? extends T> source,
			Comparator<? super T> otherComparator) {
		int n = sources.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(sources, 0, newArray, 0, n);
		newArray[n] = source;

		if (!valueComparator.equals(otherComparator)) {
			@SuppressWarnings("unchecked")
			Comparator<T> currentComparator = (Comparator<T>) this.valueComparator;
			final Comparator<T> newComparator = currentComparator.thenComparing(otherComparator);
			return new FluxMergeOrdered<>(prefetch, queueSupplier, newComparator, newArray);
		}
		return new FluxMergeOrdered<>(prefetch, queueSupplier, valueComparator, newArray);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return sources.length > 0 ? sources[0] : null;
		if (key == Attr.PREFETCH) return prefetch;
		if (key == Attr.DELAY_ERROR) return true;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MergeOrderedMainProducer<T> main = new MergeOrderedMainProducer<>(actual, valueComparator, prefetch, sources.length);
		actual.onSubscribe(main);
		main.subscribe(sources);
	}


	static final class MergeOrderedMainProducer<T> implements InnerProducer<T> {

		static final Object DONE = new Object();

		final CoreSubscriber<? super T> actual;
		final MergeOrderedInnerSubscriber<T>[] subscribers;
		final Comparator<? super T> comparator;
		final Object[] values;

		volatile Throwable error;
		static final AtomicReferenceFieldUpdater<MergeOrderedMainProducer, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(MergeOrderedMainProducer.class, Throwable.class, "error");

		volatile int cancelled;
		static final AtomicIntegerFieldUpdater<MergeOrderedMainProducer> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(MergeOrderedMainProducer.class, "cancelled");

		volatile long requested;
		static final AtomicLongFieldUpdater<MergeOrderedMainProducer> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(MergeOrderedMainProducer.class, "requested");

		volatile long emitted;
		static final AtomicLongFieldUpdater<MergeOrderedMainProducer> EMITTED =
				AtomicLongFieldUpdater.newUpdater(MergeOrderedMainProducer.class, "emitted");

		volatile int wip;
		static final AtomicIntegerFieldUpdater<MergeOrderedMainProducer> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MergeOrderedMainProducer.class, "wip");

		MergeOrderedMainProducer(CoreSubscriber<? super T> actual,
				Comparator<? super T> comparator, int prefetch, int n) {
			this.actual = actual;
			this.comparator = comparator;

			@SuppressWarnings("unchecked")
			MergeOrderedInnerSubscriber<T>[] mergeOrderedInnerSub =
					new MergeOrderedInnerSubscriber[n];
			this.subscribers = mergeOrderedInnerSub;

			for (int i = 0; i < n; i++) {
				this.subscribers[i] = new MergeOrderedInnerSubscriber<>(this, prefetch);
			}
			this.values = new Object[n];
		}

		void subscribe(Publisher<? extends T>[] sources) {
			if (sources.length != subscribers.length) {
				throw new IllegalArgumentException("must subscribe with " + subscribers.length + " sources");
			}
			for (int i = 0; i < sources.length; i++) {
				Objects.requireNonNull(sources[i], "subscribed with a null source: sources[" + i + "]")
				       .subscribe(subscribers[i]);
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public void request(long n) {
			Operators.addCap(REQUESTED, this, n);
			drain();
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				for (MergeOrderedInnerSubscriber<T> subscriber : subscribers) {
					subscriber.cancel();
				}

				if (WIP.getAndIncrement(this) == 0) {
					Arrays.fill(values, null);
					for (MergeOrderedInnerSubscriber<T> subscriber : subscribers) {
						subscriber.queue.clear();
					}
				}
			}
		}

		void onInnerError(MergeOrderedInnerSubscriber<T> inner, Throwable ex) {
			Exceptions.addThrowable(ERROR, this, ex);
			inner.done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			CoreSubscriber<? super T> actual = this.actual;
			Comparator<? super T> comparator = this.comparator;

			MergeOrderedInnerSubscriber<T>[] subscribers = this.subscribers;
			int n = subscribers.length;

			Object[] values = this.values;

			long e = emitted;

			for (;;) {
				long r = requested;

				for (;;) {
					if (cancelled != 0) {
						Arrays.fill(values, null);

						for (MergeOrderedInnerSubscriber<T> inner : subscribers) {
							inner.queue.clear();
						}
						return;
					}

					int done = 0;
					int nonEmpty = 0;
					for (int i = 0; i < n; i++) {
						Object o = values[i];
						if (o == DONE) {
							done++;
							nonEmpty++;
						}
						else if (o == null) {
							boolean innerDone = subscribers[i].done;
							o = subscribers[i].queue.poll();
							if (o != null) {
								values[i] = o;
								nonEmpty++;
							}
							else if (innerDone) {
								values[i] = DONE;
								done++;
								nonEmpty++;
							}
						}
						else {
							nonEmpty++;
						}
					}

					if (done == n) {
						Throwable ex = error;
						if (ex == null) {
							actual.onComplete();
						} else {
							actual.onError(ex);
						}
						return;
					}

					if (nonEmpty != n || e >= r) {
						break;
					}

					T min = null;
					int minIndex = -1;

					int i = 0;
					for (Object o : values) {
						if (o != DONE) {
							boolean smaller;
							try {
								@SuppressWarnings("unchecked")
								T t = (T) o;
								smaller = min == null || comparator.compare(min, t) > 0;
							} catch (Throwable ex) {
								Exceptions.addThrowable(ERROR, this, ex);
								cancel();
								actual.onError(Exceptions.terminate(ERROR, this));
								return;
							}
							if (smaller) {
								@SuppressWarnings("unchecked")
								T t = (T) o;
								min = t;
								minIndex = i;
							}
						}
						i++;
					}

					values[minIndex] = null;

					actual.onNext(min);

					e++;
					subscribers[minIndex].request(1);
				}

				this.emitted = e;
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return actual;
			if (key == Attr.CANCELLED) return this.cancelled > 0;
			if (key == Attr.ERROR) return this.error;
			if (key == Attr.DELAY_ERROR) return true;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested - emitted;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}

	static final class MergeOrderedInnerSubscriber<T> implements InnerOperator<T, T> {

		final MergeOrderedMainProducer<T> parent;
		final int prefetch;
		final int limit;
		final Queue<T> queue;

		int consumed;

		volatile boolean done;

		volatile Subscription s;
		AtomicReferenceFieldUpdater<MergeOrderedInnerSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MergeOrderedInnerSubscriber.class, Subscription.class, "s");

		MergeOrderedInnerSubscriber(MergeOrderedMainProducer<T> parent,
				int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
			this.queue = Queues.<T>small().get();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T item) {
			queue.offer(item);
			parent.drain();
		}

		@Override
		public void onError(Throwable throwable) {
			parent.onInnerError(this, throwable);
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}

		/**
		 * @param n is ignored and considered to be 1
		 */
		@Override
		public void request(long n) {
			int c = consumed + 1;
			if (c == limit) {
				consumed = 0;
				Subscription sub = s;
				if (sub != this) {
					sub.request(c);
				}
			}
			else {
				consumed = c;
			}
		}

		@Override
		public void cancel() {
			Subscription sub  = S.getAndSet(this, this);
			if (sub != null && sub != this) {
				sub.cancel();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return parent.actual; //TODO check
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key){
			if (key == Attr.ACTUAL) return parent; //TODO does that check out?
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}
}

/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

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
final class FluxMergeComparing<T> extends Flux<T> implements SourceProducer<T> {

	final int                      prefetch;
	final Comparator<? super T>    valueComparator;
	final Publisher<? extends T>[] sources;
	final boolean delayError;

	@SafeVarargs
	FluxMergeComparing(int prefetch, Comparator<? super T> valueComparator, boolean delayError, Publisher<? extends T>... sources) {
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
		this.valueComparator = valueComparator;
		this.delayError = delayError;
	}

	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources. The provided {@link Comparator} is tested for equality
	 * with the current comparator, and if different is combined by {@link Comparator#thenComparing(Comparator)}.
	 * <p>
	 * This operation doesn't change the current {@link FluxMergeComparing} instance.
	 *
	 * @param source the new source to merge with the others
	 * @return the new {@link FluxMergeComparing} instance
	 */
	FluxMergeComparing<T> mergeAdditionalSource(Publisher<? extends T> source, Comparator<? super T> otherComparator) {
		int n = sources.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(sources, 0, newArray, 0, n);
		newArray[n] = source;

		if (!valueComparator.equals(otherComparator)) {
			@SuppressWarnings("unchecked")
			Comparator<T> currentComparator = (Comparator<T>) this.valueComparator;
			final Comparator<T> newComparator = currentComparator.thenComparing(otherComparator);
			return new FluxMergeComparing<>(prefetch, newComparator, delayError, newArray);
		}
		return new FluxMergeComparing<>(prefetch, valueComparator, delayError, newArray);
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
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MergeOrderedMainProducer<T> main = new MergeOrderedMainProducer<>(actual, valueComparator, prefetch, sources.length, delayError);
		actual.onSubscribe(main);
		main.subscribe(sources);
	}


	static final class MergeOrderedMainProducer<T> implements InnerProducer<T> {

		static final Object DONE = new Object();

		final CoreSubscriber<? super T> actual;
		final MergeOrderedInnerSubscriber<T>[] subscribers;
		final Comparator<? super T> comparator;
		final Object[] values;
		final boolean delayError;

		boolean done;

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
				Comparator<? super T> comparator, int prefetch, int n, boolean delayError) {
			this.actual = actual;
			this.comparator = comparator;
			this.delayError = delayError;

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
				Objects.requireNonNull(sources[i], "subscribed with a null source: sources[" + i + "]");
				sources[i].subscribe(subscribers[i]);
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
					discardData();
				}
			}
		}

		void onInnerError(MergeOrderedInnerSubscriber<T> inner, Throwable ex) {
			Throwable e = Operators.onNextInnerError(ex, actual().currentContext(), this);
			if (e != null) {
				if (Exceptions.addThrowable(ERROR, this, e)) {
					if (!delayError) {
						done = true;
					}
					inner.done = true;
					drain();
				}
				else {
					inner.done = true;
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
			else {
				inner.done = true;
				drain();
			}
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
					boolean d = this.done;
					if (cancelled != 0) {
						Arrays.fill(values, null);

						for (MergeOrderedInnerSubscriber<T> inner : subscribers) {
							inner.queue.clear();
						}
						return;
					}

					int innerDoneCount = 0;
					int nonEmpty = 0;
					for (int i = 0; i < n; i++) {
						Object o = values[i];
						if (o == DONE) {
							innerDoneCount++;
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
								innerDoneCount++;
								nonEmpty++;
							}
						}
						else {
							nonEmpty++;
						}
					}

					if (checkTerminated(d || innerDoneCount == n, actual)) {
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

		boolean checkTerminated(boolean d, Subscriber<?> a) {
			if (cancelled != 0) {
				discardData();
				return true;
			}
			
			if (!d) {
				return false;
			}
			
			if (delayError) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					e = Exceptions.terminate(ERROR, this);
					a.onError(e);
				}
				else {
					a.onComplete();
				}
			}
			else {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					e = Exceptions.terminate(ERROR, this);
					cancel();
					discardData();
					a.onError(e);
				}
				else {
					a.onComplete();
				}
			}
			return true;
		}

		private void discardData() {
			Context ctx = actual().currentContext();
			for (Object v : values) {
				if (v != DONE) {
					Operators.onDiscard(v, ctx);
				}
			}
			Arrays.fill(values, null);
			for (MergeOrderedInnerSubscriber<T> subscriber : subscribers) {
				Operators.onDiscardQueueWithClear(subscriber.queue, ctx, null);
			}
		}
		
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return actual;
			if (key == Attr.CANCELLED) return this.cancelled > 0;
			if (key == Attr.ERROR) return this.error;
			if (key == Attr.DELAY_ERROR) return delayError;
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

		volatile     Subscription                                                           s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeOrderedInnerSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MergeOrderedInnerSubscriber.class, Subscription.class, "s");

		MergeOrderedInnerSubscriber(MergeOrderedMainProducer<T> parent, int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
			this.queue = Queues.<T>get(prefetch).get();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T item) {
			if (parent.done || done) {
				Operators.onNextDropped(item, actual().currentContext());
				return;
			}
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

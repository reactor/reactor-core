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

import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Groups upstream items into their own Publisher sequence based on a key selector.
 *
 * @param <T> the source value type
 * @param <K> the key value type
 * @param <V> the group item value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxGroupBy<T, K, V> extends InternalFluxOperator<T, GroupedFlux<K, V>>
		implements Fuseable {

	final Function<? super T, ? extends K> keySelector;

	final Function<? super T, ? extends V> valueSelector;

	final Supplier<? extends Queue<V>> groupQueueSupplier;

	final Supplier<? extends Queue<GroupedFlux<K, V>>> mainQueueSupplier;

	final int prefetch;

	FluxGroupBy(Flux<? extends T> source,
			Function<? super T, ? extends K> keySelector,
			Function<? super T, ? extends V> valueSelector,
			Supplier<? extends Queue<GroupedFlux<K, V>>> mainQueueSupplier,
			Supplier<? extends Queue<V>> groupQueueSupplier,
			int prefetch) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.keySelector = Objects.requireNonNull(keySelector, "keySelector");
		this.valueSelector = Objects.requireNonNull(valueSelector, "valueSelector");
		this.mainQueueSupplier =
				Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.groupQueueSupplier =
				Objects.requireNonNull(groupQueueSupplier, "groupQueueSupplier");
		this.prefetch = prefetch;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super GroupedFlux<K, V>> actual) {
		return new GroupByMain<>(actual,
				mainQueueSupplier.get(),
				groupQueueSupplier,
				prefetch,
				keySelector, valueSelector);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class GroupByMain<T, K, V>
			implements QueueSubscription<GroupedFlux<K, V>>,
			           InnerOperator<T, GroupedFlux<K, V>> {

		final Function<? super T, ? extends K>          keySelector;
		final Function<? super T, ? extends V>          valueSelector;
		final Queue<GroupedFlux<K, V>>                  queue;
		final Supplier<? extends Queue<V>>              groupQueueSupplier;
		final int                                       prefetch;
		final Map<K, UnicastGroupedFlux<K, V>>          groupMap;
		final CoreSubscriber<? super GroupedFlux<K, V>> actual;

		volatile int wip;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<GroupByMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(GroupByMain.class, "requested");

		volatile boolean   done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<GroupByMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(GroupByMain.class,
						Throwable.class,
						"error");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "cancelled");

		volatile int groupCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> GROUP_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "groupCount");

		Subscription s;

		volatile boolean enableAsyncFusion;

		GroupByMain(CoreSubscriber<? super GroupedFlux<K, V>> actual,
				Queue<GroupedFlux<K, V>> queue,
				Supplier<? extends Queue<V>> groupQueueSupplier,
				int prefetch,
				Function<? super T, ? extends K> keySelector,
				Function<? super T, ? extends V> valueSelector) {
			this.actual = actual;
			this.queue = queue;
			this.groupQueueSupplier = groupQueueSupplier;
			this.prefetch = prefetch;
			this.groupMap = new ConcurrentHashMap<>();
			this.keySelector = keySelector;
			this.valueSelector = valueSelector;
			GROUP_COUNT.lazySet(this, 1);
		}

		@Override
		public final CoreSubscriber<? super GroupedFlux<K, V>> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if(done){
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			K key;
			V value;

			try {
				key = Objects.requireNonNull(keySelector.apply(t), "The keySelector returned a null value");
				value = Objects.requireNonNull(valueSelector.apply(t), "The valueSelector returned a null value");
			}
			catch (Throwable ex) {
				onError(Operators.onOperatorError(s, ex, t, actual.currentContext()));
				return;
			}

			UnicastGroupedFlux<K, V> g = groupMap.get(key);

			if (g == null) {
				// if the main is cancelled, don't create new groups
				if (cancelled == 0) {
					Queue<V> q = groupQueueSupplier.get();

					GROUP_COUNT.getAndIncrement(this);
					g = new UnicastGroupedFlux<>(key, q, this, prefetch);
					g.onNext(value);
					groupMap.put(key, g);

					queue.offer(g);
					drain();
				}
			}
			else {
				g.onNext(value);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			if(done){
				return;
			}
			for (UnicastGroupedFlux<K, V> g : groupMap.values()) {
				g.onComplete();
			}
			groupMap.clear();
			done = true;
			drain();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return groupMap.values().stream();
		}

		void signalAsyncError() {
			Throwable e = Exceptions.terminate(ERROR, this); //TODO investigate if e == null
			if (e == null) {
				e = new IllegalStateException("FluxGroupBy.signalAsyncError called without error set");
			}
			groupCount = 0;
			for (UnicastGroupedFlux<K, V> g : groupMap.values()) {
				g.onError(e);
			}
			actual.onError(e);
			groupMap.clear();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				if (GROUP_COUNT.decrementAndGet(this) == 0) {
					s.cancel();
				}
				else if (!enableAsyncFusion) {
						if (WIP.getAndIncrement(this) == 0) {
							// remove queued up but unobservable groups from the mapping
							GroupedFlux<K, V> g;
							while ((g = queue.poll()) != null) {
								((UnicastGroupedFlux<K, V>) g).cancel();
							}

							if (WIP.decrementAndGet(this) == 0) {
								return;
							}

							drainLoop();
						}
				}
			}
		}

		void groupTerminated(K key) {
			if (groupCount == 0) {
				return;
			}
			groupMap.remove(key);
			if (GROUP_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (enableAsyncFusion) {
				drainFused();
			}
			else {
				drainLoop();
			}
		}

		void drainFused() {
			int missed = 1;

			final Subscriber<? super GroupedFlux<K, V>> a = actual;
			final Queue<GroupedFlux<K, V>> q = queue;

			for (; ; ) {

				if (cancelled != 0) {
					q.clear();
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						signalAsyncError();
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainLoop() {

			int missed = 1;

			Subscriber<? super GroupedFlux<K, V>> a = actual;
			Queue<GroupedFlux<K, V>> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (e != r) {
					boolean d = done;
					GroupedFlux<K, V> v = q.poll();
					boolean empty = v == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0L) {

					s.request(e);

					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d,
				boolean empty,
				Subscriber<?> a,
				Queue<GroupedFlux<K, V>> q) {
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					q.clear();
					signalAsyncError();
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
		public GroupedFlux<K, V> poll() {
			return queue.poll();
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				enableAsyncFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}
	}

	static final class UnicastGroupedFlux<K, V> extends GroupedFlux<K, V>
			implements Fuseable, QueueSubscription<V>, InnerProducer<V> {

		final K key;

		final int limit;

		final Context context;

		@Override
		public K key() {
			return key;
		}

		final Queue<V> queue;

		volatile GroupByMain<?, K, V> parent;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UnicastGroupedFlux, GroupByMain> PARENT =
				AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedFlux.class,
						GroupByMain.class,
						"parent");

		volatile boolean done;
		Throwable error;

		volatile CoreSubscriber<? super V> actual;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UnicastGroupedFlux, CoreSubscriber> ACTUAL =
				AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedFlux.class,
						CoreSubscriber.class,
						"actual");

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<UnicastGroupedFlux> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedFlux.class, "once");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<UnicastGroupedFlux> WIP =
				AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedFlux.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<UnicastGroupedFlux> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(UnicastGroupedFlux.class, "requested");

		volatile boolean outputFused;

		int produced;
		boolean isFirstRequest = true;

		UnicastGroupedFlux(K key,
				Queue<V> queue,
				GroupByMain<?, K, V> parent,
				int prefetch) {
			this.key = key;
			this.queue = queue;
			this.context = parent.currentContext();
			this.parent = parent;
			this.limit = Operators.unboundedOrLimit(prefetch);
		}

		void doTerminate() {
			GroupByMain<?, K, V> r = parent;
			if (r != null && PARENT.compareAndSet(this, r, null)) {
				r.groupTerminated(key);
			}
		}

		void drainRegular(Subscriber<? super V> a) {
			int missed = 1;

			final Queue<V> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					V t = q.poll();
					boolean empty = t == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (r == e) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0) {
					GroupByMain<?, K, V> main = parent;
					if (main != null) {
						if (this.isFirstRequest) {
							this.isFirstRequest = false;
							long toRequest = e - 1;

							if (toRequest > 0) {
								main.s.request(toRequest);
							}
						} else {
							main.s.request(e);
						}
					}
					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainFused(Subscriber<? super V> a) {
			int missed = 1;

			final Queue<V> q = queue;

			for (; ; ) {

				if (cancelled) {
					q.clear();
					actual = null;
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					actual = null;

					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain() {
			Subscriber<? super V> a = actual;
			if (a != null) {
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}

				if (outputFused) {
					drainFused(a);
				}
				else {
					drainRegular(a);
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				q.clear();
				actual = null;
				return true;
			}
			if (d && empty) {
				Throwable e = error;
				actual = null;
				if (e != null) {
					a.onError(e);
				}
				else {
					a.onComplete();
				}
				return true;
			}

			return false;
		}

		public void onNext(V t) {
			Subscriber<? super V> a = actual;

			if (!queue.offer(t)) {
				onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						actual.currentContext()));
				return;
			}
			if (outputFused) {
				if (a != null) {
					a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
				}
			}
			else {
				drain();
			}
		}

		public void onError(Throwable t) {
			error = t;
			done = true;

			doTerminate();

			drain();
		}

		public void onComplete() {
			done = true;

			doTerminate();

			drain();
		}

		@Override
		public void subscribe(CoreSubscriber<? super V> actual) {
			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
				actual.onSubscribe(this);
				ACTUAL.lazySet(this, actual);
				drain();
			}
			else {
				actual.onError(new IllegalStateException(
						"GroupedFlux allows only one Subscriber"));
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			doTerminate();

			if (!outputFused) {
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}

		@Override
		@Nullable
		public V poll() {
			V v = queue.poll();
			if (v != null) {
				produced++;
			}
			else {
				tryReplenish();
			}
			return v;
		}

		void tryReplenish() {
			int p = produced;
			if (p != 0) {
				produced = 0;
				GroupByMain<?, K, V> main = parent;
				if (main != null) {
					if (this.isFirstRequest) {
						this.isFirstRequest = false;
						p--;

						if (p > 0) {
							main.s.request(p);
						}
					} else {
						main.s.request(p);
					}
				}
			}
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			if (queue.isEmpty()) {
				tryReplenish();
				return true;
			}
			return false;
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super V> actual() {
			return actual;
		}

	}
}

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxConcatMap.ErrorMode;

/**
 * Maps each upstream value into a Publisher and concatenates them into one
 * sequence of items.
 * 
 * @param <T> the source value type
 * @param <R> the output value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxMergeSequential<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final int maxConcurrency;

	final int prefetch;

	final ErrorMode errorMode;

	public FluxMergeSequential(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			int maxConcurrency,
			int prefetch,
			ErrorMode errorMode) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.maxConcurrency = maxConcurrency;
		this.prefetch = prefetch;
		this.errorMode = errorMode;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper, false)) {
			return;
		}

		Subscriber<T> parent = null;
		switch (errorMode) {
			case END:
				parent = new MergeSequentialDelayed<T, R>(s, mapper, maxConcurrency,
						prefetch,	true);
				break;
			case IMMEDIATE:
				//TODO undelayed subscription
				throw new UnsupportedOperationException("IMMEDIATE error mode for " +
						"mergeSequential is not yet implemented");
//				break;
			case BOUNDARY:
				parent = new MergeSequentialDelayed<T, R>(s, mapper, maxConcurrency,
						prefetch, false);
		}
		source.subscribe(parent);
	}

	static final class MergeSequentialDelayed<T, R>
			implements Subscriber<T>, FluxMergeSequentialSupport<R>, Subscription {

		/** the downstream subscriber */
		final Subscriber<? super R> actual;

		final MergeSequentialInner<R> inner;

		/** the mapper giving the inner publisher for each source value */
		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		/** how many eagerly subscribed inner stream at a time, at most */
		final int maxConcurrency;

		/** request size for inner subscribers (size of the inner queues) */
		final int prefetch;

		/** the limit at which a prefetch is triggered in inners, to optimize the flow */
		final int limit;

		/** whether or not errors should be delayed until the very end of all inner
		 * publishers or just until the completion of the currently merged inner publisher
		 */
		final boolean veryEnd;

		Subscription s;

		int consumed;

		volatile Queue<T> queue;

		volatile boolean done;

		volatile boolean cancelled;

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeSequentialDelayed, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(MergeSequentialDelayed.class, Throwable.class, "error");

		volatile boolean active;

		/** guard against multiple threads entering the drain loop. allows thread
		 * stealing by continuing the loop if wip has been incremented externally by
		 * a separate thread. */
		volatile int wip;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeSequentialDelayed> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MergeSequentialDelayed.class, "wip");

		public MergeSequentialDelayed(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				int maxConcurrency,
				int prefetch,
				boolean veryEnd) {
			this.actual = actual;
			this.mapper = mapper;
			this.maxConcurrency = maxConcurrency;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
			this.veryEnd = veryEnd;
			this.inner = new MergeSequentialInner<>(this);
		}

		@Override
		public void innerNext(R value) {

		}

		@Override
		public void innerComplete() {

		}

		@Override
		public void innerError(Throwable e) {

		}

		@Override
		public void onSubscribe(Subscription s) {

		}

		@Override
		public void onNext(T t) {

		}

		@Override
		public void onError(Throwable t) {

		}

		@Override
		public void onComplete() {

		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	}

	static final class MergeSequentialInner<R>
			extends Operators.MultiSubscriptionSubscriber<R, R> {

		private final FluxMergeSequentialSupport<R> parent;

		public MergeSequentialInner(FluxMergeSequentialSupport<R> parent) {
			super(null);
			this.parent = parent;
		}

		@Override
		public void onNext(R r) {

		}
	}

	interface FluxMergeSequentialSupport<T> {

		void innerNext(T value);

		void innerComplete();

		void innerError(Throwable e);
	}
}
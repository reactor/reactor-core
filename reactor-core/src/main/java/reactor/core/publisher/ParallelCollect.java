/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <C> the collection type
 */
final class ParallelCollect<T, C> extends ParallelFlux<C> implements Scannable, Fuseable {

	final ParallelFlux<? extends T> source;

	final Supplier<? extends C> initialCollection;

	final BiConsumer<? super C, ? super T> collector;

	ParallelCollect(ParallelFlux<? extends T> source,
			Supplier<? extends C> initialCollection,
			BiConsumer<? super C, ? super T> collector) {
		this.source = ParallelFlux.from(source);
		this.initialCollection = initialCollection;
		this.collector = collector;
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		if (key == InternalProducerAttr.INSTANCE) return true;

		return null;
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void subscribe(CoreSubscriber<? super C>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}

		int n = subscribers.length;
		@SuppressWarnings("unchecked") CoreSubscriber<T>[] parents = new CoreSubscriber[n];

		for (int i = 0; i < n; i++) {

			C initialValue;

			try {
				initialValue = Objects.requireNonNull(initialCollection.get(),
						"The initialSupplier returned a null value");
			}
			catch (Throwable ex) {
				reportError(subscribers, Operators.onOperatorError(ex,
						subscribers[i].currentContext()));
				return;
			}

			parents[i] = new ParallelCollectSubscriber<>(subscribers[i],
					initialValue,
					collector);
		}

		source.subscribe(parents);
	}

	void reportError(Subscriber<?>[] subscribers, Throwable ex) {
		for (Subscriber<?> s : subscribers) {
			Operators.error(s, ex);
		}
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	static final class ParallelCollectSubscriber<T, C>
			extends Operators.BaseFluxToMonoOperator<T, C> {

		final BiConsumer<? super C, ? super T> collector;

		C collection;

		boolean done;

		ParallelCollectSubscriber(CoreSubscriber<? super C> subscriber,
				C initialValue,
				BiConsumer<? super C, ? super T> collector) {
			super(subscriber);
			this.collection = initialValue;
			this.collector = collector;
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			try {
				synchronized (this) {
					final C collection = this.collection;
					if (collection != null) {
						collector.accept(collection, t);
					}
				}
			}
			catch (Throwable ex) {
				onError(Operators.onOperatorError(this.s, ex, t, actual.currentContext()));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;

			final C c;
			synchronized (this) {
				c = collection;
				collection = null;
			}

			if (c == null) {
				return;
			}

			Operators.onDiscard(c, actual.currentContext());

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			completePossiblyEmpty();
		}

		@Override
		public void cancel() {
			s.cancel();

			final C c;
			synchronized (this) {
				c = collection;
				collection = null;
			}

			if (c != null) {
				Operators.onDiscard(c, actual.currentContext());
			}
		}

		@Override
		C accumulatedValue() {
			final C c;
			synchronized (this) {
				c = collection;
				collection = null;
			}
			return c;
		}

		@Override
		public @Nullable Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return collection == null && !done;
			return super.scanUnsafe(key);
		}
	}
}

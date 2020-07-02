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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

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
		this.source = source;
		this.initialCollection = initialCollection;
		this.collector = collector;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
			extends Operators.MonoSubscriber<T, C> {

		final BiConsumer<? super C, ? super T> collector;

		C collection;

		Subscription s;

		boolean done;

		ParallelCollectSubscriber(CoreSubscriber<? super C> subscriber,
				C initialValue,
				BiConsumer<? super C, ? super T> collector) {
			super(subscriber);
			this.collection = initialValue;
			this.collector = collector;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			try {
				collector.accept(collection, t);
			}
			catch (Throwable ex) {
				onError(Operators.onOperatorError(this, ex, t, actual.currentContext()));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			collection = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			C c = collection;
			collection = null;
			complete(c);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}
	}
}

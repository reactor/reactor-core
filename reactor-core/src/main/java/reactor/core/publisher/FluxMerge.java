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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * Merges a fixed array of Publishers.
 * @param <T> the element type of the publishers
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxMerge<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] sources;

	final boolean delayError;

	final boolean isMergeWith;

	final int maxConcurrency;
	
	final Supplier<? extends Queue<T>> mainQueueSupplier;

	final int prefetch;
	
	final Supplier<? extends Queue<T>> innerQueueSupplier;

	FluxMerge(Publisher<? extends T>[] sources,
			boolean delayError, int maxConcurrency,
			Supplier<? extends Queue<T>> mainQueueSupplier,
			int prefetch,
			Supplier<? extends Queue<T>> innerQueueSupplier) {

		this(sources, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier, false);
	}
	
	FluxMerge(Publisher<? extends T>[] sources,
			boolean delayError, int maxConcurrency, 
			Supplier<? extends Queue<T>> mainQueueSupplier, 
			int prefetch,
			Supplier<? extends Queue<T>> innerQueueSupplier,
			boolean isMergeWith) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		if (maxConcurrency <= 0) {
			throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
		}
		this.isMergeWith = isMergeWith;
		this.sources = Objects.requireNonNull(sources, "sources");
		this.delayError = delayError;
		this.maxConcurrency = maxConcurrency;
		this.prefetch = prefetch;
		this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
	}
	
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		FluxFlatMap.FlatMapMain<Publisher<? extends T>, T> merger;

		if (isMergeWith) {
			merger = new FluxMergeWithMain<>(
					actual, identityFunction(), delayError, maxConcurrency, mainQueueSupplier, prefetch,
					innerQueueSupplier);

		}
		else {
			merger = new FluxFlatMap.FlatMapMain<>(
					actual, identityFunction(), delayError, maxConcurrency, mainQueueSupplier, prefetch,
					innerQueueSupplier);
		}
		
		merger.onSubscribe(new FluxArray.ArraySubscription<>(merger, sources));
	}
	
	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxMerge instance.
	 * 
	 * @param source the new source to merge with the others
	 * @param newQueueSupplier a function that should return a new queue supplier based on the change in the maxConcurrency value
	 * @return the new FluxMerge instance
	 */
	FluxMerge<T> mergeAdditionalSource(Publisher<? extends T> source, IntFunction<Supplier<? extends Queue<T>>> newQueueSupplier) {
		int n = sources.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(sources, 0, newArray, 0, n);
		newArray[n] = source;
		
		// increase the maxConcurrency because if merged separately, it would have run concurrently anyway
		Supplier<? extends Queue<T>> newMainQueue;
		int mc = maxConcurrency;
		if (mc != Integer.MAX_VALUE) {
			mc++;
			newMainQueue = newQueueSupplier.apply(mc);
		} else {
			newMainQueue = mainQueueSupplier;
		}
		
		return new FluxMerge<>(newArray, delayError, mc, newMainQueue, prefetch, innerQueueSupplier, isMergeWith);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.PREFETCH) return prefetch;

		return null;
	}

	static final class FluxMergeWithMain<T> extends FluxFlatMap.FlatMapMain<Publisher<? extends T>, T> {

		volatile FluxFlatMap.FlatMapInner<T> main;

		static final AtomicReferenceFieldUpdater<FluxMergeWithMain, FluxFlatMap.FlatMapInner> MAIN =
				AtomicReferenceFieldUpdater.newUpdater(FluxMergeWithMain.class, FluxFlatMap.FlatMapInner.class, "main");

		FluxMergeWithMain(CoreSubscriber<? super T> actual,
				Function<? super Publisher<? extends T>, ? extends Publisher<? extends T>> mapper,
				boolean delayError,
				int maxConcurrency,
				Supplier<? extends Queue<T>> mainQueueSupplier,
				int prefetch,
				Supplier<? extends Queue<T>> innerQueueSupplier) {
			  super(actual,
					mapper,
					delayError,
					maxConcurrency,
					mainQueueSupplier,
					prefetch,
					innerQueueSupplier);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return main;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(Publisher<? extends T> t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			Publisher<? extends T> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null Publisher");
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
				if (e_ != null) {
					onError(e_);
				}
				else {
					tryEmitScalar(null);
				}
				return;
			}

			if (p instanceof Callable) {
				T v;
				try {
					v = ((Callable<T>) p).call();
				}
				catch (Throwable e) {
					//does the strategy apply? if so, short-circuit the delayError. In any case, don't cancel
					Throwable e_ = Operators.onNextPollError(t, e, actual.currentContext());
					if (e_ == null) {
						return;
					}
					//now if error mode strategy doesn't apply, let delayError play
					if (!delayError || !Exceptions.addThrowable(ERROR, this, e)) {
						onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
					}

					return;
				}
				tryEmitScalar(v);
			}
			else {
				FluxFlatMap.FlatMapInner<T> inner = new FluxFlatMap.FlatMapInner<>(this, prefetch);
				if (add(inner)) {
					if (main == null) {
						MAIN.compareAndSet(this, null, inner);
					}

					p.subscribe(inner);
				}
			}
		}
	}
}
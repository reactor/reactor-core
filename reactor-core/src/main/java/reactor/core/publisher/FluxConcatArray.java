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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxConcatArray<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;
	
	final boolean delayError;

	@SafeVarargs
	FluxConcatArray(boolean delayError, Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.delayError = delayError;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Publisher<? extends T>[] a = array;

		if (a.length == 0) {
			Operators.complete(actual);
			return;
		}
		if (a.length == 1) {
			Publisher<? extends T> p = a[0];

			if (p == null) {
				Operators.error(actual, new NullPointerException("The single source Publisher is null"));
			} else {
				p.subscribe(actual);
			}
			return;
		}

		if (delayError) {
			ConcatArrayDelayErrorSubscriber<T> parent = new
					ConcatArrayDelayErrorSubscriber<>(actual, a);

			actual.onSubscribe(parent);

			if (!parent.isCancelled()) {
				parent.onComplete();
			}
			return;
		}
		ConcatArraySubscriber<T> parent = new ConcatArraySubscriber<>(actual, a);

		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
	}


	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxMerge instance.
	 * 
	 * @param source the new source to merge with the others
	 * @return the new FluxConcatArray instance
	 */
	FluxConcatArray<T> concatAdditionalSourceLast(Publisher<? extends T> source) {
		int n = array.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(array, 0, newArray, 0, n);
		newArray[n] = source;
		
		return new FluxConcatArray<>(delayError, newArray);
	}

	/**
	 * Returns a new instance which has the additional source to be merged together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxMerge instance.
	 *
	 * @param source the new source to merge with the others
	 * @return the new FluxConcatArray instance
	 */
	@SuppressWarnings("unchecked")
	<V> FluxConcatArray<V> concatAdditionalIgnoredLast(Publisher<? extends V>
			source) {
		int n = array.length;
		Publisher<? extends V>[] newArray = new Publisher[n + 1];
		//noinspection SuspiciousSystemArraycopy
		System.arraycopy(array, 0, newArray, 0, n);
		newArray[n - 1] = Mono.ignoreElements(newArray[n - 1]);
		newArray[n] = source;

		return new FluxConcatArray<>(delayError, newArray);
	}

	/**
	 * Returns a new instance which has the additional first source to be concatenated together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxConcatArray instance.
	 * 
	 * @param source the new source to merge with the others
	 * @return the new FluxConcatArray instance
	 */
	FluxConcatArray<T> concatAdditionalSourceFirst(Publisher<? extends T> source) {
		int n = array.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[n + 1];
		System.arraycopy(array, 0, newArray, 1, n);
		newArray[0] = source;
		
		return new FluxConcatArray<>(delayError, newArray);
	}

	
	static final class ConcatArraySubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Publisher<? extends T>[] sources;

		int index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatArraySubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(ConcatArraySubscriber.class, "wip");

		long produced;

		ConcatArraySubscriber(CoreSubscriber<? super T> actual, Publisher<? extends T>[]
				sources) {
			super(actual);
			this.sources = sources;
		}

		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Publisher<? extends T>[] a = sources;
				do {

					if (isCancelled()) {
						return;
					}

					int i = index;
					if (i == a.length) {
						actual.onComplete();
						return;
					}

					Publisher<? extends T> p = a[i];

					if (p == null) {
						actual.onError(new NullPointerException("Source Publisher at index " + i + " is null"));
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}
					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

					index = ++i;
				} while (WIP.decrementAndGet(this) != 0);
			}

		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}
	}

	static final class ConcatArrayDelayErrorSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Publisher<? extends T>[] sources;

		int index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatArrayDelayErrorSubscriber> WIP =
		AtomicIntegerFieldUpdater.newUpdater(ConcatArrayDelayErrorSubscriber.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ConcatArrayDelayErrorSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ConcatArrayDelayErrorSubscriber.class, Throwable.class, "error");
		
		long produced;

		ConcatArrayDelayErrorSubscriber(CoreSubscriber<? super T> actual, Publisher<?
				extends T>[] sources) {
			super(actual);
			this.sources = sources;
		}

		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				onComplete();
			} else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.DELAY_ERROR) return true;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Publisher<? extends T>[] a = sources;
				do {

					if (isCancelled()) {
						return;
					}

					int i = index;
					if (i == a.length) {
						Throwable e = Exceptions.terminate(ERROR, this);
						if (e != null) {
							actual.onError(e);
						} else {
							actual.onComplete();
						}
						return;
					}

					Publisher<? extends T> p = a[i];

					if (p == null) {
						actual.onError(new NullPointerException("Source Publisher at index " + i + " is null"));
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}
					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

					index = ++i;
				} while (WIP.decrementAndGet(this) != 0);
			}

		}
	}

}

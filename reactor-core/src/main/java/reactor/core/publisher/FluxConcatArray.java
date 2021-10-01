/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
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

	static final Object WORKING = new Object();
	static final Object DONE = new Object();

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

			parent.onComplete();
			return;
		}
		ConcatArraySubscriber<T> parent = new ConcatArraySubscriber<>(actual, a);

		parent.onComplete();
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

	interface SubscriptionAware {
		Subscription upstream();
	}
	
	static final class ConcatArraySubscriber<T> extends ThreadLocal<Object> implements InnerOperator<T, T>, SubscriptionAware {

		final CoreSubscriber<? super T> actual;
		final Publisher<? extends T>[] sources;

		int index;
		long produced;
		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ConcatArraySubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ConcatArraySubscriber.class, "requested");

		volatile boolean cancelled;

		ConcatArraySubscriber(CoreSubscriber<? super T> actual, Publisher<? extends T>[] sources) {
			this.actual = actual;
			this.sources = sources;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (this.cancelled) {
				this.remove();
				s.cancel();
				return;
			}

			final Subscription previousSubscription = this.s;

			this.s = s;

			if (previousSubscription == null) {
				this.actual.onSubscribe(this);
				return;
			}

			final long actualRequested = activateAndGetRequested(REQUESTED, this);
			if (actualRequested > 0) {
				s.request(actualRequested);
			}
		}

		@Override
		public void onNext(T t) {
			this.produced++;

			this.actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			this.remove();
			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (this.get() == WORKING) {
				this.set(DONE);
				return;
			}

			final Publisher<? extends T>[] a = this.sources;

			for (;;) {
				this.set(WORKING);

				int i = this.index;
				if (i == a.length) {
					this.remove();

					if (this.cancelled) {
						return;
					}

					this.actual.onComplete();
					return;
				}

				Publisher<? extends T> p = a[i];

				if (p == null) {
					this.remove();

					if (this.cancelled) {
						return;
					}

					this.actual.onError(new NullPointerException("Source Publisher at index " + i + " is null"));
					return;
				}

				long c = this.produced;
				if (c != 0L) {
					this.produced = 0L;
					deactivateAndProduce(c, REQUESTED, this);
				}

				this.index = ++i;

				if (this.cancelled) {
					return;
				}
				p.subscribe(this);

				final Object state = this.get();
				if (state != DONE) {
					this.remove();
					return;
				}
			}
		}

		@Override
		public void request(long n) {
			final Subscription subscription = addCapAndGetSubscription(n, REQUESTED, this);

			if (subscription == null) {
				return;
			}

			subscription.request(n);
		}

		@Override
		public void cancel() {
			this.remove();

			this.cancelled = true;

			if ((this.requested & Long.MIN_VALUE) != Long.MIN_VALUE) {
				this.s.cancel();
			}
		}

		@Override
		public Subscription upstream() {
			return this.s;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PARENT) return this.s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class ConcatArrayDelayErrorSubscriber<T> extends ThreadLocal<Object> implements InnerOperator<T, T>, SubscriptionAware {

		final CoreSubscriber<? super T> actual;
		final Publisher<? extends T>[] sources;

		int index;
		long produced;
		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ConcatArrayDelayErrorSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ConcatArrayDelayErrorSubscriber.class, "requested");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ConcatArrayDelayErrorSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ConcatArrayDelayErrorSubscriber.class, Throwable.class, "error");

		volatile boolean cancelled;

		ConcatArrayDelayErrorSubscriber(CoreSubscriber<? super T> actual, Publisher<? extends T>[] sources) {
			this.actual = actual;
			this.sources = sources;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (this.cancelled) {
				this.remove();
				s.cancel();
				return;
			}

			final Subscription previousSubscription = this.s;

			this.s = s;

			if (previousSubscription == null) {
				this.actual.onSubscribe(this);
				return;
			}

			final long actualRequested = activateAndGetRequested(REQUESTED, this);
			if (actualRequested > 0) {
				s.request(actualRequested);
			}
		}

		@Override
		public void onNext(T t) {
			this.produced++;

			this.actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (!Exceptions.addThrowable(ERROR, this, t)) {
				this.remove();
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			onComplete();
		}

		@Override
		public void onComplete() {
			if (this.get() == WORKING) {
				this.set(DONE);
				return;
			}

			final Publisher<? extends T>[] a = this.sources;

			for (;;) {
				this.set(WORKING);

				int i = this.index;
				if (i == a.length) {
					this.remove();

					final Throwable e = Exceptions.terminate(ERROR, this);
					if (e == Exceptions.TERMINATED) {
						return;
					}

					if (e != null) {
						this.actual.onError(e);
					} else {
						this.actual.onComplete();
					}
					return;
				}

				final Publisher<? extends T> p = a[i];

				if (p == null) {
					this.remove();

					if (this.cancelled) {
						return;
					}

					final NullPointerException npe = new NullPointerException("Source Publisher at index " + i + " is null");
					if (!Exceptions.addThrowable(ERROR, this, npe)) {
						Operators.onErrorDropped(npe, this.actual.currentContext());
						return;
					}

					final Throwable throwable = Exceptions.terminate(ERROR, this);
					if (throwable == Exceptions.TERMINATED) {
						return;
					}

					this.actual.onError(throwable);
					return;
				}

				long c = this.produced;
				if (c != 0L) {
					this.produced = 0L;
					deactivateAndProduce(c, REQUESTED, this);
				}

				this.index = ++i;

				if (this.cancelled) {
					return;
				}

				p.subscribe(this);

				final Object state = this.get();
				if (state != DONE) {
					this.remove();
					return;
				}
			}
		}

		@Override
		public void request(long n) {
			final Subscription subscription = addCapAndGetSubscription(n, REQUESTED, this);

			if (subscription == null) {
				return;
			}

			subscription.request(n);
		}

		@Override
		public void cancel() {
			this.remove();

			this.cancelled = true;

			if ((this.requested & Long.MIN_VALUE) != Long.MIN_VALUE) {
				this.s.cancel();
			}

			final Throwable throwable = Exceptions.terminate(ERROR, this);
			if (throwable != null) {
				Operators.onErrorDropped(throwable, this.actual.currentContext());
			}
		}

		@Override
		public Subscription upstream() {
			return this.s;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.DELAY_ERROR) return true;
			if (key == Attr.TERMINATED) return this.error == Exceptions.TERMINATED;
			if (key == Attr.ERROR) return this.error != Exceptions.TERMINATED ? this.error : null;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.PARENT) return this.s;
			if (key == Attr.CANCELLED) return this.cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return this.requested;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static <T> long activateAndGetRequested(AtomicLongFieldUpdater<T> updater, T instance) {
		for (;;) {
			final long deactivatedRequested = updater.get(instance);
			final long actualRequested = deactivatedRequested & Long.MAX_VALUE;

			if (updater.compareAndSet(instance, deactivatedRequested, actualRequested)) {
				return actualRequested;
			}
		}
	}

	static <T> void deactivateAndProduce(long produced, AtomicLongFieldUpdater<T> updater, T instance) {
		for (;;) {
			final long actualRequested = updater.get(instance);
			final long deactivatedRequested = actualRequested == Long.MAX_VALUE
					? Long.MAX_VALUE | Long.MIN_VALUE
					: (actualRequested - produced) | Long.MIN_VALUE;

			if (updater.compareAndSet(instance, actualRequested, deactivatedRequested)) {
				return;
			}
		}
	}

	@Nullable
	static <T extends SubscriptionAware> Subscription addCapAndGetSubscription(long n, AtomicLongFieldUpdater<T> updater, T instance) {
		for (;;) {
			final long state = updater.get(instance);
			final Subscription s = instance.upstream();
			final long actualRequested = state & Long.MAX_VALUE;
			final long status = state & Long.MIN_VALUE;

			if (actualRequested == Long.MAX_VALUE) {
				return status == Long.MIN_VALUE ? null : s;
			}

			if (updater.compareAndSet(instance, state, Operators.addCap(actualRequested , n) | status)) {
				return status == Long.MIN_VALUE ? null : s;
			}
		}
	}

}

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Waits for all Mono sources to produce a value or terminate, and if all of them produced
 * a value, emit a Tuples of those values; otherwise terminate.
 *
 * @param <R> the source value types
 */
final class MonoZip<T, R> extends Mono<R> implements SourceProducer<R>  {

	final boolean delayError;

	final Publisher<?>[] sources;

	final Iterable<? extends Publisher<?>> sourcesIterable;

	final Function<? super Object[], ? extends R> zipper;

	@SuppressWarnings("unchecked")
	<U> MonoZip(boolean delayError,
			Publisher<? extends T> p1,
			Publisher<? extends U> p2,
			BiFunction<? super T, ? super U, ? extends R> zipper2) {
		this(delayError,
				new FluxZip.PairwiseZipper<>(new BiFunction[]{
						Objects.requireNonNull(zipper2, "zipper2")}),
				Objects.requireNonNull(p1, "p1"),
				Objects.requireNonNull(p2, "p2"));
	}

	MonoZip(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Publisher<?>... sources) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = Objects.requireNonNull(sources, "sources");
		this.sourcesIterable = null;
	}

	MonoZip(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Iterable<? extends Publisher<?>> sourcesIterable) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = null;
		this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
	}

	@SuppressWarnings("unchecked")
	@Nullable
	Mono<R> zipAdditionalSource(Publisher source, BiFunction zipper) {
		Publisher[] oldSources = sources;
		if (oldSources != null && this.zipper instanceof FluxZip.PairwiseZipper) {
			int oldLen = oldSources.length;
			Publisher<?>[] newSources = new Publisher[oldLen + 1];
			System.arraycopy(oldSources, 0, newSources, 0, oldLen);
			newSources[oldLen] = source;

			Function<Object[], R> z =
					((FluxZip.PairwiseZipper<R>) this.zipper).then(zipper);

			return new MonoZip<>(delayError, z, newSources);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super R> actual) {
		Publisher<?>[] a;
		int n = 0;
		if (sources != null) {
			a = sources;
			n = a.length;
		}
		else {
			a = new Publisher[8];
			for (Publisher<?> m : sourcesIterable) {
				if (n == a.length) {
					Publisher<?>[] b = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, b, 0, n);
					a = b;
				}
				a[n++] = m;
			}
		}

		if (n == 0) {
			Operators.complete(actual);
			return;
		}

		ZipCoordinator<R> parent = new ZipCoordinator<>(actual, n, delayError, zipper);
		actual.onSubscribe(parent);
		ZipInner<R>[] subs = parent.subscribers;
		for (int i = 0; i < n; i++) {
			a[i].subscribe(subs[i]);
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class ZipCoordinator<R> extends Operators.MonoSubscriber<Object, R> {

		final ZipInner<R>[] subscribers;

		final boolean delayError;

		final Function<? super Object[], ? extends R> zipper;

		volatile int done;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ZipCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(ZipCoordinator.class, "done");

		@SuppressWarnings("unchecked")
		ZipCoordinator(CoreSubscriber<? super R> subscriber,
				int n,
				boolean delayError,
				Function<? super Object[], ? extends R> zipper) {
			super(subscriber);
			this.delayError = delayError;
			this.zipper = zipper;
			subscribers = new ZipInner[n];
			for (int i = 0; i < n; i++) {
				subscribers[i] = new ZipInner<>(this);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) {
				return done == subscribers.length;
			}
			if (key == Attr.BUFFERED) {
				return subscribers.length;
			}
			if (key == Attr.DELAY_ERROR) {
				return delayError;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}

			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@SuppressWarnings("unchecked")
		void signal() {
			ZipInner<R>[] a = subscribers;
			int n = a.length;
			if (DONE.incrementAndGet(this) != n) {
				return;
			}

			Object[] o = new Object[n];
			Throwable error = null;
			Throwable compositeError = null;
			boolean hasEmpty = false;

			for (int i = 0; i < a.length; i++) {
				ZipInner<R> m = a[i];
				Object v = m.value;
				if (v != null) {
					o[i] = v;
				}
				else {
					Throwable e = m.error;
					if (e != null) {
						if (compositeError != null) {
							//this is ok as the composite created below is never a singleton
							compositeError.addSuppressed(e);
						}
						else if (error != null) {
							compositeError = Exceptions.multiple(error, e);
						}
						else {
							error = e;
						}
					}
					else {
						hasEmpty = true;
					}
				}
			}

			if (compositeError != null) {
				actual.onError(compositeError);
			}
			else if (error != null) {
				actual.onError(error);
			}
			else if (hasEmpty) {
				actual.onComplete();
			}
			else {
				R r;
				try {
					r = Objects.requireNonNull(zipper.apply(o),
							"zipper produced a null value");
				}
				catch (Throwable t) {
					actual.onError(Operators.onOperatorError(null,
							t,
							o,
							actual.currentContext()));
					return;
				}
				complete(r);
			}
		}

		@Override
		public void cancel() {
			if (!isCancelled()) {
				super.cancel();
				for (ZipInner<R> ms : subscribers) {
					ms.cancel();
				}
			}
		}

		void cancelExcept(ZipInner<R> source) {
			if (!isCancelled()) {
				super.cancel();
				for (ZipInner<R> ms : subscribers) {
					if(ms != source) {
						ms.cancel();
					}
				}
			}
		}
	}

	static final class ZipInner<R> implements InnerConsumer<Object> {

		final ZipCoordinator<R> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ZipInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ZipInner.class,
						Subscription.class,
						"s");

		Object    value;
		Throwable error;

		ZipInner(ZipCoordinator<R> parent) {
			this.parent = parent;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) {
				return s == Operators.cancelledSubscription();
			}
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.ACTUAL) {
				return parent;
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}

			return null;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
			else {
				s.cancel();
			}
		}

		@Override
		public void onNext(Object t) {
			if (value == null) {
				value = t;
				parent.signal();
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			if (parent.delayError) {
				parent.signal();
			}
			else {
				int n = parent.subscribers.length;
				if (ZipCoordinator.DONE.getAndSet(parent, n) != n) {
					parent.cancelExcept(this);
					parent.actual.onError(t);
				}
			}
		}

		@Override
		public void onComplete() {
			if (value == null) {
				if (parent.delayError) {
					parent.signal();
				}
				else {
					int n = parent.subscribers.length;
					if (ZipCoordinator.DONE.getAndSet(parent, n) != n) {
						parent.cancelExcept(this);
						parent.actual.onComplete();
					}
				}
			}
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}

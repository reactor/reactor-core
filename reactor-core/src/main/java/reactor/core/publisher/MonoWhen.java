/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
 */
final class MonoWhen extends Mono<Void> implements SourceProducer<Void>  {

	final boolean delayError;

	final Publisher<?>[] sources;

	final Iterable<? extends Publisher<?>> sourcesIterable;

	MonoWhen(boolean delayError, Publisher<?>... sources) {
		this.delayError = delayError;
		this.sources = Objects.requireNonNull(sources, "sources");
		this.sourcesIterable = null;
	}

	MonoWhen(boolean delayError, Iterable<? extends Publisher<?>> sourcesIterable) {
		this.delayError = delayError;
		this.sources = null;
		this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
	}

	@SuppressWarnings("unchecked")
	@Nullable
	Mono<Void> whenAdditionalSource(Publisher<?> source) {
		Publisher[] oldSources = sources;
		if (oldSources != null) {
			int oldLen = oldSources.length;
			Publisher<?>[] newSources = new Publisher[oldLen + 1];
			System.arraycopy(oldSources, 0, newSources, 0, oldLen);
			newSources[oldLen] = source;

			return new MonoWhen(delayError, newSources);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super Void> actual) {
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

		WhenCoordinator parent = new WhenCoordinator(actual, n, delayError);
		actual.onSubscribe(parent);
		parent.subscribe(a);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	static final class WhenCoordinator extends Operators.MonoSubscriber<Object, Void> {

		final WhenInner[] subscribers;

		final boolean delayError;

		volatile int done;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WhenCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(WhenCoordinator.class, "done");

		@SuppressWarnings("unchecked")
		WhenCoordinator(CoreSubscriber<? super Void> subscriber,
				int n,
				boolean delayError) {
			super(subscriber);
			this.delayError = delayError;
			subscribers = new WhenInner[n];
			for (int i = 0; i < n; i++) {
				subscribers[i] = new WhenInner(this);
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

		void subscribe(Publisher<?>[] sources) {
			WhenInner[] a = subscribers;
			for (int i = 0; i < a.length; i++) {
				sources[i].subscribe(a[i]);
			}
		}

		void signalError(Throwable t) {
			if (delayError) {
				signal();
			}
			else {
				int n = subscribers.length;
				if (DONE.getAndSet(this, n) != n) {
					cancel();
					actual.onError(t);
				}
			}
		}

		@SuppressWarnings("unchecked")
		void signal() {
			WhenInner[] a = subscribers;
			int n = a.length;
			if (DONE.incrementAndGet(this) != n) {
				return;
			}

			Throwable error = null;
			Throwable compositeError = null;

			for (int i = 0; i < a.length; i++) {
				WhenInner m = a[i];
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

			}

			if (compositeError != null) {
				actual.onError(compositeError);
			}
			else if (error != null) {
				actual.onError(error);
			}
			else {
				actual.onComplete();
			}
		}

		@Override
		public void cancel() {
			if (!isCancelled()) {
				super.cancel();
				for (WhenInner ms : subscribers) {
					ms.cancel();
				}
			}
		}
	}

	static final class WhenInner implements InnerConsumer<Object> {

		final WhenCoordinator parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WhenInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(WhenInner.class,
						Subscription.class,
						"s");
		Throwable error;

		WhenInner(WhenCoordinator parent) {
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
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			parent.signalError(t);
		}

		@Override
		public void onComplete() {
			parent.signal();
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}

/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
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

		WhenCoordinator parent = new WhenCoordinator(a, actual, n, delayError);
		actual.onSubscribe(parent);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	static final class WhenCoordinator implements InnerProducer<Void>,
	                                              Fuseable,
	                                              Fuseable.QueueSubscription<Void> {

		final CoreSubscriber<? super Void> actual;
		final Publisher<?>[] sources;
		final WhenInner[] subscribers;

		final boolean delayError;

		volatile long state;
		static final AtomicLongFieldUpdater<WhenCoordinator> STATE =
				AtomicLongFieldUpdater.newUpdater(WhenCoordinator.class, "state");

		static final long INTERRUPTED_FLAG    =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long REQUESTED_ONCE_FLAG =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long MAX_SIGNALS_VALUE   =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0111_1111_1111_1111_1111_1111_1111_1111L;

		WhenCoordinator(
				Publisher<?>[] sources,
				CoreSubscriber<? super Void> actual,
				int n,
				boolean delayError) {
			this.sources = sources;
			this.actual = actual;
			this.delayError = delayError;
			subscribers = new WhenInner[n];
			for (int i = 0; i < n; i++) {
				subscribers[i] = new WhenInner(this);
			}
		}

		@Override
		public CoreSubscriber<? super Void> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return deliveredSignals(this.state) == subscribers.length;
			if (key == Attr.BUFFERED) return subscribers.length;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			if (key == Attr.CANCELLED) {
				final long state = this.state;
				return isInterrupted(state) && deliveredSignals(state) != subscribers.length;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		boolean signal() {
			final WhenInner[] a = subscribers;
			int n = a.length;

			final long previousState = markDeliveredSignal(this);
			final int deliveredSignals = deliveredSignals(previousState);
			if (isInterrupted(previousState) || deliveredSignals == n) {
				return false;
			}

			if ((deliveredSignals + 1) != n) {
				return true;
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

			return true;
		}

		@Override
		public void request(long n) {
			final long previousState = markRequestedOnce(this);
			if (isRequestedOnce(previousState) || isInterrupted(previousState)) {
				return;
			}

			final Publisher<?>[] sources = this.sources;
			final WhenInner[] subs = this.subscribers;
			for (int i = 0; i < subscribers.length; i++) {
				sources[i].subscribe(subs[i]);
			}
		}

		@Override
		public void cancel() {
			final long previousState = markInterrupted(this);
			if (isInterrupted(previousState) || !isRequestedOnce(previousState) || deliveredSignals(previousState) == subscribers.length) {
				return;
			}

			for (WhenInner ms : subscribers) {
				ms.cancel();
			}
		}

		void cancelExcept(WhenInner source) {
			for (WhenInner ms : subscribers) {
				if (ms != source) {
					ms.cancel();
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public Void poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}

		static long markRequestedOnce(WhenCoordinator instance) {
			for (;;) {
				final long state = instance.state;

				if (isInterrupted(state) || isRequestedOnce(state)) {
					return state;
				}

				final long nextState = state | REQUESTED_ONCE_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long markDeliveredSignal(WhenCoordinator instance) {
			final int n = instance.subscribers.length;
			for (;;) {
				final long state = instance.state;

				if (isInterrupted(state) || n == deliveredSignals(state)) {
					return state;
				}

				final long nextState = state + 1;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long markForceTerminated(WhenCoordinator instance) {
			final int n = instance.subscribers.length;
			for (;;) {
				final long state = instance.state;

				if (isInterrupted(state) || n == deliveredSignals(state)) {
					return state;
				}

				final long nextState = (state &~ MAX_SIGNALS_VALUE) | n | INTERRUPTED_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static long markInterrupted(WhenCoordinator instance) {
			final int n = instance.subscribers.length;
			for (;;) {
				final long state = instance.state;

				if (isInterrupted(state) || n == deliveredSignals(state)) {
					return state;
				}

				final long nextState = state | INTERRUPTED_FLAG;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return state;
				}
			}
		}

		static boolean isRequestedOnce(long state) {
			return (state & REQUESTED_ONCE_FLAG) == REQUESTED_ONCE_FLAG;
		}

		static int deliveredSignals(long state) {
			return (int) (state & Integer.MAX_VALUE);
		}

		static boolean isInterrupted(long state) {
			return (state & INTERRUPTED_FLAG) == INTERRUPTED_FLAG;
		}
	}

	static final class WhenInner implements InnerConsumer<Object> {

		final WhenCoordinator parent;

		volatile Subscription s;
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
			return parent.actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Object t) {
			Operators.onDiscard(t, currentContext());
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			if (parent.delayError) {
				if (!parent.signal()) {
					Operators.onErrorDropped(t, parent.actual.currentContext());
				}
			}
			else {
				final long previousState = WhenCoordinator.markForceTerminated(parent);
				if (WhenCoordinator.isInterrupted(previousState)) {
					return;
				}

				parent.cancelExcept(this);
				parent.actual.onError(t);
			}
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

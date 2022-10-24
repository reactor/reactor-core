/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

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
 *
 * @param <R> the source value types
 */
final class MonoZip<T, R> extends Mono<R> implements SourceProducer<R>  {

	final boolean delayError;

	final Mono<?>[] sources;

	final Iterable<? extends Mono<?>> sourcesIterable;

	final Function<? super Object[], ? extends R> zipper;

	@SuppressWarnings("unchecked")
	<U> MonoZip(boolean delayError,
			Mono<? extends T> p1,
			Mono<? extends U> p2,
			BiFunction<? super T, ? super U, ? extends R> zipper2) {
		this(delayError,
				new FluxZip.PairwiseZipper<>(new BiFunction[]{
						Objects.requireNonNull(zipper2, "zipper2")}),
				Objects.requireNonNull(p1, "p1"),
				Objects.requireNonNull(p2, "p2"));
	}

	MonoZip(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Mono<?>... sources) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = Objects.requireNonNull(sources, "sources");
		this.sourcesIterable = null;
	}

	MonoZip(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Iterable<? extends Mono<?>> sourcesIterable) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = null;
		this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
	}

	@SuppressWarnings("unchecked")
	@Nullable
	Mono<R> zipAdditionalSource(Mono source, BiFunction zipper) {
		Mono[] oldSources = sources;
		if (oldSources != null && this.zipper instanceof FluxZip.PairwiseZipper) {
			int oldLen = oldSources.length;
			Mono<?>[] newSources = new Mono[oldLen + 1];
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
		Mono<?>[] a;
		int n = 0;
		if (sources != null) {
			a = sources;
			n = a.length;
		}
		else {
			a = new Mono[8];
			for (Mono<?> m : sourcesIterable) {
				if (n == a.length) {
					Mono<?>[] b = new Mono[n + (n >> 2)];
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

		actual.onSubscribe(new ZipCoordinator<>(a, actual, n, delayError, zipper));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.DELAY_ERROR) return delayError;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class ZipCoordinator<R> implements InnerProducer<R>,
	                                                Fuseable,
	                                                Fuseable.QueueSubscription<R> {


		final Mono<?>[] sources;

		final ZipInner<R>[] subscribers;

		final CoreSubscriber<? super R> actual;

		final boolean delayError;

		final Function<? super Object[], ? extends R> zipper;

		volatile long state;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ZipCoordinator> STATE =
				AtomicLongFieldUpdater.newUpdater(ZipCoordinator.class, "state");

		static final long INTERRUPTED_FLAG    =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long REQUESTED_ONCE_FLAG =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
		static final long MAX_SIGNALS_VALUE   =
				0b0000_0000_0000_0000_0000_0000_0000_0000_0111_1111_1111_1111_1111_1111_1111_1111L;

		@SuppressWarnings("unchecked")
		ZipCoordinator(
				Mono<?>[] sources,
				CoreSubscriber<? super R> subscriber,
				int n,
				boolean delayError,
				Function<? super Object[], ? extends R> zipper) {
			this.sources = sources;
			this.actual = subscriber;
			this.delayError = delayError;
			this.zipper = zipper;
			final ZipInner<R>[] ss = new ZipInner[n];
			this.subscribers = ss;
			for (int i = 0; i < n; i++) {
				ss[i] = new ZipInner<>(this);
			}
		}

		@Override
		public CoreSubscriber<? super R> actual() {
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
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public void request(long n) {
			final long previousState = markRequestedOnce(this);
			if (isRequestedOnce(previousState) || isInterrupted(previousState)) {
				return;
			}

			final Mono<?>[] monos = this.sources;
			final ZipInner<R>[] subs = this.subscribers;
			for (int i = 0; i < subscribers.length; i++) {
				monos[i].subscribe(subs[i]);
			}
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		boolean signal() {
			ZipInner<R>[] a = subscribers;
			int n = a.length;

			final long previousState = markDeliveredSignal(this);
			final int deliveredSignals = deliveredSignals(previousState);
			if (isInterrupted(previousState) || deliveredSignals == n) {
				return false;
			}

			if ((deliveredSignals + 1) != n) {
				return true;
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
					Operators.onDiscardMultiple(Arrays.asList(o), actual.currentContext());
					actual.onError(Operators.onOperatorError(null,
							t,
							o,
							actual.currentContext()));
					return true;
				}
				actual.onNext(r);
				actual.onComplete();
			}

			return true;
		}

		@Override
		public void cancel() {
			final long previousState = markInterrupted(this);
			if (isInterrupted(previousState) || !isRequestedOnce(previousState) || deliveredSignals(previousState) == subscribers.length) {
				return;
			}

			final Context context = actual.currentContext();
			for (ZipInner<R> ms : subscribers) {
				if (ms.cancel()) {
					Operators.onDiscard(ms.value, context);
				}
			}
		}

		void cancelExcept(ZipInner<R> source) {
			final Context context = actual.currentContext();
			for (ZipInner<R> ms : subscribers) {
				if (ms != source && ms.cancel()) {
					Operators.onDiscard(ms.value, context);
				}
			}
		}

		@Override
		public R poll() {
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

		static <T> long markRequestedOnce(ZipCoordinator<T> instance) {
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

		static <T> long markDeliveredSignal(ZipCoordinator<T> instance) {
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

		static <T> long markForceTerminated(ZipCoordinator<T> instance) {
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

		static <T> long markInterrupted(ZipCoordinator<T> instance) {
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

	static final class ZipInner<R> implements InnerConsumer<Object> {

		final ZipCoordinator<R> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ZipInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ZipInner.class, Subscription.class, "s");

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
			if (value == null) {
				value = t;
				parent.signal();
				/*
				 We use cancelledSubscription() as a marker to detect whether a value is present in the cancelAll/cancelExcept parent phase.
				 This is done in order to deal with a single volatile.
				 Of course, it could also be set that way due to an early cancellation (in which case we very much want to discard the t).
				 Note the accumulator should already have seen and used the value in parent.signal().
				 We don't really cancel the subscription, but we don't really care because that's a Mono. Having received onNext, we're sure
				 to get onComplete afterwards.
				 See also boolean cancel().
				 */
				final Subscription a = this.s;
				if (a != Operators.cancelledSubscription() && S.compareAndSet(this, a, Operators.cancelledSubscription())) {
					return;
				}
				Operators.onDiscard(t, parent.actual.currentContext());
			}
		}

		@Override
		public void onError(Throwable t) {
			if (value != null) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}

			error = t;
			if (parent.delayError) {
				if (!parent.signal()) {
					Operators.onErrorDropped(t, parent.actual.currentContext());
				}
			}
			else {
				final long previousState = ZipCoordinator.markForceTerminated(parent);
				if (ZipCoordinator.isInterrupted(previousState)) {
					return;
				}

				parent.cancelExcept(this);
				parent.actual.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (value != null) {
				return;
			}

			if (parent.delayError) {
				parent.signal();
			}
			else {
				final long previousState = ZipCoordinator.markForceTerminated(parent);
				if (ZipCoordinator.isInterrupted(previousState)) {
					return;
				}

				parent.cancelExcept(this);
				parent.actual.onComplete();
			}
		}

		boolean cancel() {
			/*
			If S == cancelledSubscription, it means we've either already cancelled (nothing to do) or previously signalled an onNext.
			In both cases, terminate will return false (having failed to swap to cancelledSubscription) and the method will return true.
			This is to be interpreted by parent callers (cancelAll/cancelExcept) as an indicator that a value is likely present,
			and that it should be discarded by the parent. Parent could try to discard twice, in the case of double cancellation, but
			discard should be idempotent.
			 */
			return !Operators.terminate(S, this);
		}
	}
}

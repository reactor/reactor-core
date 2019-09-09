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
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Shares a {@link Mono} for the duration of a function that may transform it and
 * consume it as many times as necessary without causing multiple subscriptions
 * to the upstream.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoPublishMulticast<T, R> extends InternalMonoOperator<T, R> implements Fuseable {

	final Function<? super Mono<T>, ? extends Mono<? extends R>> transform;

	MonoPublishMulticast(Mono<? extends T> source,
			Function<? super Mono<T>, ? extends Mono<? extends R>> transform) {
		super(source);
		this.transform = Objects.requireNonNull(transform, "transform");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		MonoPublishMulticaster<T> multicast = new MonoPublishMulticaster<>(actual.currentContext());

		Mono<? extends R> out;
		try {
			out = Objects.requireNonNull(transform.apply(fromDirect(multicast)),
					"The transform returned a null Mono");
		}
		catch (Throwable ex) {
			Operators.error(actual, Operators.onOperatorError(ex, actual.currentContext()));
			return null;
		}

		if (out instanceof Fuseable) {
			out.subscribe(new FluxPublishMulticast.CancelFuseableMulticaster<>(actual, multicast));
		}
		else {
			out.subscribe(new FluxPublishMulticast.CancelMulticaster<>(actual, multicast));
		}

		return multicast;
	}

	static final class MonoPublishMulticaster<T> extends Mono<T>
			implements InnerConsumer<T>, FluxPublishMulticast.PublishMulticasterParent {

		volatile     Subscription s;
		static final AtomicReferenceFieldUpdater<MonoPublishMulticaster, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MonoPublishMulticaster.class,
						Subscription.class,
						"s");

		volatile     int wip;
		static final AtomicIntegerFieldUpdater<MonoPublishMulticaster> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MonoPublishMulticaster.class, "wip");

		volatile PublishMulticastInner<T>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoPublishMulticaster, PublishMulticastInner[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(MonoPublishMulticaster.class, PublishMulticastInner[].class, "subscribers");

		@SuppressWarnings("rawtypes")
		static final PublishMulticastInner[] EMPTY = new PublishMulticastInner[0];

		@SuppressWarnings("rawtypes")
		static final PublishMulticastInner[] TERMINATED = new PublishMulticastInner[0];

		volatile boolean done;

		@Nullable
		T         value;
		Throwable error;

		volatile boolean connected;

		final Context context;

		@SuppressWarnings("unchecked")
		MonoPublishMulticaster(Context ctx) {
			SUBSCRIBERS.lazySet(this, EMPTY);
			this.context = ctx;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.CANCELLED) {
				return s == Operators.cancelledSubscription();
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.PREFETCH) {
				return 1;
			}
			if (key == Attr.BUFFERED) {
				return value != null ? 1 : 0;
			}

			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public Context currentContext() {
			return context;
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> actual) {
			PublishMulticastInner<T> pcs = new PublishMulticastInner<>(this, actual);
			actual.onSubscribe(pcs);

			if (add(pcs)) {
				if (pcs.cancelled == 1) {
					remove(pcs);
					return;
				}
				drain();
			}
			else {
				Throwable ex = error;
				if (ex != null) {
					actual.onError(ex);
				}
				else {
					actual.onComplete();
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				connected = true;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, context);
				return;
			}

			value = t;
			done = true;
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, context);
				return;
			}
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {
				if (connected) {
					if (s == Operators.cancelledSubscription()) {
						value = null;
						return;
					}

					final T v = value;

					PublishMulticastInner<T>[] a = subscribers;
					int n = a.length;

					if (n != 0) {
						if (s == Operators.cancelledSubscription()) {
							value = null;
							return;
						}

						if (v == null) {
							@SuppressWarnings("unchecked")
							PublishMulticastInner<T>[] castedArray = SUBSCRIBERS.getAndSet(this, TERMINATED);
							a = castedArray;
							n = a.length;
							for (int i = 0; i < n; i++) {
								a[i].actual.onComplete();
							}
							return;
						}
						else {
							@SuppressWarnings("unchecked")
							PublishMulticastInner<T>[] castedArray = SUBSCRIBERS.getAndSet(this, TERMINATED);
							a = castedArray;
							n = a.length;
							for (int i = 0; i < n; i++) {
								a[i].actual.onNext(v);
								a[i].actual.onComplete();
							}
							value = null;
							return;
						}
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean add(PublishMulticastInner<T> s) {
			for (; ; ) {
				PublishMulticastInner<T>[] a = subscribers;

				if (a == TERMINATED) {
					return false;
				}

				int n = a.length;

				@SuppressWarnings("unchecked")
				PublishMulticastInner<T>[] b = new PublishMulticastInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = s;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		@SuppressWarnings("unchecked")
		void remove(PublishMulticastInner<T> s) {
			for (; ; ) {
				PublishMulticastInner<T>[] a = subscribers;

				if (a == TERMINATED || a == EMPTY) {
					return;
				}

				int n = a.length;
				int j = -1;

				for (int i = 0; i < n; i++) {
					if (a[i] == s) {
						j = i;
						break;
					}
				}

				if (j < 0) {
					return;
				}

				PublishMulticastInner<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new PublishMulticastInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}

		@Override
		public void terminate() {
			Operators.terminate(S, this);
			if (WIP.getAndIncrement(this) == 0) {
				if (connected) {
					value = null;
				}
			}
		}
	}

	static final class PublishMulticastInner<T> implements InnerProducer<T> {

		final MonoPublishMulticaster<T> parent;

		final CoreSubscriber<? super T> actual;

		volatile int cancelled;
		static final AtomicIntegerFieldUpdater<PublishMulticastInner> CANCELLED = AtomicIntegerFieldUpdater.newUpdater(PublishMulticastInner.class, "cancelled");

		PublishMulticastInner(MonoPublishMulticaster<T> parent,
				CoreSubscriber<? super T> actual) {
			this.parent = parent;
			this.actual = actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.CANCELLED) {
				return cancelled == 1;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				parent.drain();
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				parent.remove(this);
				parent.drain();
			}
		}
	}

}

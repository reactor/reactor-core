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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static reactor.core.publisher.Operators.cancelledSubscription;

final class MonoSequenceEqual<T> extends Mono<Boolean> {
	final Publisher<? extends T>            first;
	final Publisher<? extends T>            second;
	final BiPredicate<? super T, ? super T> comparer;
	final int                               bufferSize;

	MonoSequenceEqual(Publisher<? extends T> first, Publisher<? extends T> second,
			BiPredicate<? super T, ? super T> comparer, int bufferSize) {
		this.first = Objects.requireNonNull(first, "first");
		this.second = Objects.requireNonNull(second, "second");
		this.comparer = Objects.requireNonNull(comparer, "comparer");
		if(bufferSize < 1){
			throw new IllegalArgumentException("Buffer size must be strictly positive: " +
					""+bufferSize);
		}
		this.bufferSize = bufferSize;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Boolean> actual) {
		EqualCoordinator<T> ec = new EqualCoordinator<>(actual, bufferSize, first, second, comparer);
		actual.onSubscribe(ec);
		ec.subscribe();
	}

	static final class EqualCoordinator<T> implements InnerProducer<Boolean> {
		final CoreSubscriber<? super Boolean> actual;
		final BiPredicate<? super T, ? super T> comparer;
		final Publisher<? extends T> first;
		final Publisher<? extends T> second;
		final EqualSubscriber<T> firstSubscriber;
		final EqualSubscriber<T> secondSubscriber;

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<EqualCoordinator> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(EqualCoordinator.class, "once");

		T v1;

		T v2;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<EqualCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(EqualCoordinator.class, "wip");

		EqualCoordinator(CoreSubscriber<? super Boolean> actual, int bufferSize,
				Publisher<? extends T> first, Publisher<? extends T> second,
				BiPredicate<? super T, ? super T> comparer) {
			this.actual = actual;
			this.first = first;
			this.second = second;
			this.comparer = comparer;
			firstSubscriber = new EqualSubscriber<>(this, bufferSize);
			secondSubscriber = new EqualSubscriber<>(this, bufferSize);
		}

		@Override
		public CoreSubscriber<? super Boolean> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(firstSubscriber, secondSubscriber);
		}

		void subscribe() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				first.subscribe(firstSubscriber);
				second.subscribe(secondSubscriber);
			}
		}

		@Override
		public void request(long n) {
			if (!Operators.validate(n)) {
				return;
			}
			if (ONCE.compareAndSet(this, 0, 1)) {
				first.subscribe(firstSubscriber);
				second.subscribe(secondSubscriber);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				cancelInner(firstSubscriber);
				cancelInner(secondSubscriber);

				if (WIP.getAndIncrement(this) == 0) {
					firstSubscriber.queue.clear();
					secondSubscriber.queue.clear();
				}
			}
		}

		void cancel(EqualSubscriber<T> s1, Queue<T> q1, EqualSubscriber<T> s2, Queue<T> q2) {
			cancelled = true;
			cancelInner(s1);
			q1.clear();
			cancelInner(s2);
			q2.clear();
		}

		void cancelInner(EqualSubscriber<T> innerSubscriber) {
			Subscription s = innerSubscriber.subscription;
			if (s != cancelledSubscription()) {
				s = EqualSubscriber.S.getAndSet(innerSubscriber,
						cancelledSubscription());
				if (s != null && s != cancelledSubscription()) {
					s.cancel();
				}
			}
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			final EqualSubscriber<T> s1 = firstSubscriber;
			final Queue<T> q1 = s1.queue;
			final EqualSubscriber<T> s2 = secondSubscriber;
			final Queue<T> q2 = s2.queue;

			for (;;) {

				long r = 0L;
				for (;;) {
					if (cancelled) {
						q1.clear();
						q2.clear();
						return;
					}

					boolean d1 = s1.done;

					if (d1) {
						Throwable e = s1.error;
						if (e != null) {
							cancel(s1, q1, s2, q2);

							actual.onError(e);
							return;
						}
					}

					boolean d2 = s2.done;

					if (d2) {
						Throwable e = s2.error;
						if (e != null) {
							cancel(s1, q1, s2, q2);

							actual.onError(e);
							return;
						}
					}

					if (v1 == null) {
						v1 = q1.poll();
					}
					boolean e1 = v1 == null;

					if (v2 == null) {
						v2 = q2.poll();
					}
					boolean e2 = v2 == null;

					if (d1 && d2 && e1 && e2) {
						actual.onNext(true);
						actual.onComplete();
						return;
					}
					if ((d1 && d2) && (e1 != e2)) {
						cancel(s1, q1, s2, q2);

						actual.onNext(false);
						actual.onComplete();
						return;
					}

					if (!e1 && !e2) {
						boolean c;

						try {
							c = comparer.test(v1, v2);
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							cancel(s1, q1, s2, q2);

							actual.onError(Operators.onOperatorError(ex,
									actual.currentContext()));
							return;
						}

						if (!c) {
							cancel(s1, q1, s2, q2);

							actual.onNext(false);
							actual.onComplete();
							return;
						}
						r++;

						v1 = null;
						v2 = null;
					}

					if (e1 || e2) {
						break;
					}
				}

				if (r != 0L) {
					s1.cachedSubscription.request(r);
					s2.cachedSubscription.request(r);
				}


				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
	}

	static final class EqualSubscriber<T>
			implements InnerConsumer<T> {
		final EqualCoordinator<T> parent;
		final Queue<T>            queue;
		final int                 bufferSize;

		volatile boolean done;
		Throwable error;

		Subscription cachedSubscription;
		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<EqualSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(EqualSubscriber.class,
						Subscription.class, "subscription");

		EqualSubscriber(EqualCoordinator<T> parent, int bufferSize) {
			this.parent = parent;
			this.bufferSize = bufferSize;
			this.queue = Queues.<T>get(bufferSize).get();
		}

		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.ERROR) return error;
			if (key == Attr.CANCELLED) return subscription == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.PREFETCH) return bufferSize;
			if (key == Attr.BUFFERED) return queue.size();

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				this.cachedSubscription = s;
				s.request(bufferSize);
			}
		}

		@Override
		public void onNext(T t) {
			if (!queue.offer(t)) {
				onError(Operators.onOperatorError(cachedSubscription, Exceptions
						.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						currentContext()));
				return;
			}
			parent.drain();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			parent.drain();
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}
	}
}

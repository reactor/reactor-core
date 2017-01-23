/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Trackable;
import reactor.core.publisher.FluxBufferPredicate.Mode;

/**
 * Cut a sequence into non-overlapping windows where each window boundary is determined by
 * a {@link Predicate} on the values. The predicate can be used in several modes:
 * <ul>
 *     <li>{@code Until}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the last in the previous window.</li>
 *     <li>{@code UntilOther}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the first in the new window.</li>
 *     <li>{@code While}: A new window starts when the predicate stops matching. The
 *     non-matching elements that delimit each window are simply discarded, and the
 *     windows are not emitted before an inner element is pushed</li>
 * </ul>
 *
 * @param <T> the source and window value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxWindowPredicate<T>
		extends FluxSource<T, Flux<T>> {

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	final Predicate<? super T> predicate;

	final Mode mode;

	public FluxWindowPredicate(Publisher<? extends T> source, Predicate<? super T> predicate,
			Supplier<? extends Queue<T>> processorQueueSupplier, Mode mode) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
		this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.mode = mode;
	}

	@Override
	public void subscribe(Subscriber<? super Flux<T>> s) {
		source.subscribe(new WindowPredicateSubscriber<T>(s, predicate, mode, processorQueueSupplier));
	}

	static final class WindowPredicateSubscriber<T>
			extends AbstractQueue<T>
			implements Fuseable.ConditionalSubscriber<T>, Subscription, Trackable,
			           BooleanSupplier, Disposable {

		final Subscriber<? super Flux<T>> actual;

		final Mode mode;

		final Predicate<? super T> predicate;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		UnicastProcessor<T> window;

		boolean done;

		volatile int once;
		static final AtomicIntegerFieldUpdater<WindowPredicateSubscriber>
				ONCE =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateSubscriber.class, "once");

		volatile boolean fastpath;

		volatile long requested;
		static final AtomicLongFieldUpdater<WindowPredicateSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowPredicateSubscriber.class, "requested");

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<WindowPredicateSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(WindowPredicateSubscriber.class, Subscription.class, "s");

		WindowPredicateSubscriber(Subscriber<? super Flux<T>> actual,
				Predicate<? super T> predicate, Mode mode,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.processorQueueSupplier = processorQueueSupplier;
			this.predicate = predicate;
			this.mode = mode;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (n == Long.MAX_VALUE) {
					// here we request everything from the source. switching to
					// fastpath will avoid unnecessary request(1) during filling
					fastpath = true;
					requested = Long.MAX_VALUE;
					s.request(Long.MAX_VALUE);
				}
				else {
					// Requesting from source may have been interrupted if downstream
					// received enough buffer (requested == 0), so this new request for
					// buffer should resume progressive filling from upstream. We can
					// directly request the same as the number of needed buffers (if
					// buffers turn out 1-sized then we'll have everything, otherwise
					// we'll continue requesting one by one)
//					if (!DrainUtils.postCompleteRequest(n,
//							actual,
//							this,
//							REQUESTED,
//							this,
//							this)) {
//						s.request(1);
//					}
					Operators.getAndAddCap(REQUESTED, this, n);
					s.request(1);
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			if (ONCE.compareAndSet(this, 0, 1)) {
				onNextNewWindow(); //create the initial buffer
				if (window == null) {
					//buffer creation has failed, shortcircuit
					return true;
				}
			}

			UnicastProcessor<T> w = window;
			boolean match;
			try {
				match = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			boolean requestMore;
			if (mode == Mode.UNTIL && match) {
				w.onNext(t);
				requestMore = onNextNewWindow();
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				requestMore = onNextNewWindow();
				w = window;
				w.onNext(t);
			}
			else if (mode == Mode.WHILE && !match) {
				requestMore = onNextNewWindow();
			}
			else {
				w.onNext(t);
				if (!fastpath && requested != 0) {
					requestMore = true;
				}
				else {
					requestMore = false;
				}
			}

			return !requestMore;
		}

		private UnicastProcessor<T> triggerNewWindow() {
			//we'll create a new queue for the new window
			Queue<T> q;
			try {
				q = processorQueueSupplier.get();
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e));
				return null;
			}

			if (q == null) {
				cancel();
				onError(new NullPointerException("The processorQueueSupplier returned a null queue"));
				return null;
			}

			window = new UnicastProcessor<>(q, this);
			return window;
		}

		/**
		 * @return true if requests should continue to be made
		 */
		private boolean onNextNewWindow() {
			UnicastProcessor<T> w = window;
			window = null;
			if (w != null) {
				w.onComplete();
			}
			w = triggerNewWindow();
			if (w != null) {
				return emit(w);
			}
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;

			UnicastProcessor<T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			UnicastProcessor<T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		/**
		 * @return true if requests should continue to be made
		 */
		boolean emit(Flux<T> w) {
			if (fastpath) {
				actual.onNext(w);
				return false;
			}
			long r = REQUESTED.getAndDecrement(this);
			if (r > 0) {
				actual.onNext(w);
				return requested > 0;
			}
			cancel();
			actual.onError(Exceptions.failWithOverflow("Could not emit buffer due to lack of requests"));
			return false;
		}

		@Override
		public void dispose() {
			//TODO
//			if (WIP.decrementAndGet(this) == 0) {
//				s.cancel();
//			}
		}

		@Override
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public boolean isCancelled() {
			return s == Operators.CancelledSubscription.INSTANCE;
		}

		@Override
		public long getPending() {
			UnicastProcessor<T> w = window;
			return w != null ? w.size() : 0L;
		}

		@Override
		public boolean getAsBoolean() {
			return isCancelled();
		}

		@Override
		public void clear() {
			UnicastProcessor<T> w = window;
			if (w == null) {
				return;
			}
			w.clear();
		}

		@Override
		public Iterator<T> iterator() {
			UnicastProcessor<T> w = window;
			if (w == null) {
				return Collections.emptyIterator();
			}
			//note: UnsupportedOperationException in UnicastProcessor
			return w.iterator();
		}

		@Override
		public boolean offer(T t) {
			UnicastProcessor<T> w = window;
			//note: UnsupportedOperationException in UnicastProcessor
			return w != null && w.offer(t);
		}

		@Override
		public T poll() {
			UnicastProcessor<T> w = window;
			return w == null ? null : w.poll();
		}

		@Override
		public T peek() {
			//note: UnsupportedOperationException in UnicastProcessor
			UnicastProcessor<T> w = window;
			return w == null ? null : w.peek();
		}

		@Override
		public int size() {
			UnicastProcessor<T> w = window;
			return w == null ? 0 : w.size();
		}

		@Override
		public String toString() {
			return "FluxWindowPredicate";
		}
	}
}

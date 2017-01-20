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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.FluxBufferPredicate.Mode;
import reactor.util.concurrent.QueueSupplier;

/**
 * Cut a sequence into non-overlapping windows where each window boundary is determined by
 * a {@link Predicate} on the values. The predicate can be used in several modes:
 * <ul>
 *     <li>{@code Until}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the last in the previous window.</li>
 *     <li>{@code UntilOther}: A new window starts when the predicate returns true. The
 *     element that just matched the predicate is the first in the new window.</li>
 *     <li>{@code While}: A new window starts when the predicate stops matching. The
 *     non-matching elements that delimit each window are simply discarded.</li>
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
		source.subscribe(new WindowPredicateSubscriber<>(s, predicate, mode, processorQueueSupplier));
	}

	static final class WindowPredicateSubscriber<T>
			implements Subscriber<T>, Subscription, Disposable, Producer, Receiver,
			           MultiProducer, Trackable {

		final Subscriber<? super Flux<T>> actual;

		final Mode mode;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final Predicate<? super T> predicate;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateSubscriber>
				WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateSubscriber.class, "wip");

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowPredicateSubscriber> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(WindowPredicateSubscriber.class, "once");

		boolean done;

		boolean newWindow;

		boolean pendingEmit;

		Subscription s;

		UnicastProcessor<T> window;

		public WindowPredicateSubscriber(Subscriber<? super Flux<T>> actual, Predicate<? super T> predicate,
				Mode mode, Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.mode = mode;
			this.predicate = predicate;
			this.processorQueueSupplier = processorQueueSupplier;
			this.wip = 1;
			this.newWindow = true;
			this.pendingEmit = false;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}

			if (newWindow) {
				if (!triggerNewWindow()) {
					return;
				}
			}

			UnicastProcessor<T> w = window;
			boolean match;
			try {
				match = predicate.test(t);
			}
			catch (Throwable ex) {
				WIP.decrementAndGet(this);
				done = true;
				cancel();
				actual.onError(ex);
				return;
			}

			if (mode == Mode.UNTIL && match) {
				newWindow = true;
				window = null;
				w.onNext(t);
				w.onComplete();
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				w.onComplete();
				if (!triggerNewWindow()) {
					return;
				}
				w = window;
				w.onNext(t);
			}
			else if (mode == Mode.WHILE && !match) {
				w.onComplete();
				if (!triggerNewWindow()) {
					return;
				}
			}
			else {
				if (pendingEmit) {
					pendingEmit = false;
					actual.onNext(w);
				}
				w.onNext(t);
			}
		}

		private boolean triggerNewWindow() {
			newWindow = false;
			WIP.getAndIncrement(this);

			Queue<T> q;

			try {
				q = processorQueueSupplier.get();
			}
			catch (Throwable ex) {
				WIP.decrementAndGet(this);
				done = true;
				cancel();

				actual.onError(ex);
				return false;
			}

			if (q == null) {
				WIP.decrementAndGet(this);
				done = true;
				cancel();

				actual.onError(new NullPointerException(
						"The processorQueueSupplier returned a null queue"));
				return false;
			}

			UnicastProcessor<T> w = new UnicastProcessor<>(q, this);
			window = w;
			if (mode == Mode.WHILE) {
				//we're not sure there will be an element to emit in that window, let's defer
				pendingEmit = true;
			}
			else {
				//there should be at least one element in that window, let's emit it
				actual.onNext(w);
			}
			return true;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			Processor<T, T> w = window;
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

			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				s.request(n);
			}
		}

		@Override
		public void cancel() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WIP.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public Object downstream() {
			return actual;
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
		public Object upstream() {
			return s;
		}

		@Override
		public Iterator<?> downstreams() {
			return Collections.singletonList(window)
			                  .iterator();
		}

		@Override
		public long downstreamCount() {
			return window != null ? 1L : 0L;
		}

		@Override
		public String toString() {
			return "FluxWindowPredicate";
		}
	}
}

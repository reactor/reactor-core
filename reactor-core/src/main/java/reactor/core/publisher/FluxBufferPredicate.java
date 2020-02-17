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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Buffers elements into custom collections where the buffer boundary is determined by
 * a {@link java.util.function.Predicate} on the values. The predicate can be used in
 * several modes:
 * <ul>
 *     <li>{@code Until}: A new buffer starts when the predicate returns true. The
 *     element that just matched the predicate is the last in the previous buffer.</li>
 *     <li>{@code UntilOther}: A new buffer starts when the predicate returns true. The
 *     element that just matched the predicate is the first in the new buffer.</li>
 *     <li>{@code While}: A new buffer starts when the predicate stops matching. The
 *     non-matching elements are simply discarded.</li>
 * </ul>
 *
 * @param <T> the source value type
 * @param <C> the output collection type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBufferPredicate<T, C extends Collection<? super T>>
		extends InternalFluxOperator<T, C> {

	public enum Mode {
		UNTIL, UNTIL_CUT_BEFORE, WHILE
	}

	final Predicate<? super T> predicate;

	final Supplier<C> bufferSupplier;

	final Mode mode;

	FluxBufferPredicate(Flux<? extends T> source, Predicate<? super T> predicate,
			Supplier<C> bufferSupplier, Mode mode) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.mode = mode;
	}

	@Override
	public int getPrefetch() {
		return 1; //this operator changes the downstream request to 1 in the source
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		C initialBuffer;

		try {
			initialBuffer = Objects.requireNonNull(bufferSupplier.get(),
					"The bufferSupplier returned a null initial buffer");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		BufferPredicateSubscriber<T, C> parent = new BufferPredicateSubscriber<>(actual,
				initialBuffer, bufferSupplier, predicate, mode);

		return parent;
	}

	static final class BufferPredicateSubscriber<T, C extends Collection<? super T>>
			extends AbstractQueue<C>
			implements ConditionalSubscriber<T>, InnerOperator<T, C>, BooleanSupplier {

		final CoreSubscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final Mode mode;

		final Predicate<? super T> predicate;

		C buffer;

		boolean done;

		volatile boolean fastpath;

		volatile long requestedBuffers;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferPredicateSubscriber> REQUESTED_BUFFERS =
				AtomicLongFieldUpdater.newUpdater(BufferPredicateSubscriber.class,
						"requestedBuffers");

		volatile long requestedFromSource;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferPredicateSubscriber> REQUESTED_FROM_SOURCE =
				AtomicLongFieldUpdater.newUpdater(BufferPredicateSubscriber.class,
						"requestedFromSource");

		volatile Subscription s;

		static final AtomicReferenceFieldUpdater<BufferPredicateSubscriber,
				Subscription> S = AtomicReferenceFieldUpdater.newUpdater
				(BufferPredicateSubscriber.class, Subscription.class, "s");

		BufferPredicateSubscriber(CoreSubscriber<? super C> actual, C initialBuffer,
				Supplier<C> bufferSupplier, Predicate<? super T> predicate, Mode mode) {
			this.actual = actual;
			this.buffer = initialBuffer;
			this.bufferSupplier = bufferSupplier;
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
					requestedBuffers = Long.MAX_VALUE;
					requestedFromSource = Long.MAX_VALUE;
					s.request(Long.MAX_VALUE);
				}
				else {
					// Requesting from source may have been interrupted if downstream
					// received enough buffer (requested == 0), so this new request for
					// buffer should resume progressive filling from upstream. We can
					// directly request the same as the number of needed buffers (if
					// buffers turn out 1-sized then we'll have everything, otherwise
					// we'll continue requesting one by one)
					if (!DrainUtils.postCompleteRequest(n,
							actual,
							this, REQUESTED_BUFFERS,
							this,
							this)) {
						Operators.addCap(REQUESTED_FROM_SOURCE, this, n);
						s.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			cleanup();
			Operators.terminate(S, this);
			Operators.onDiscardMultiple(buffer, actual.currentContext());
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
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			C b = buffer;
			boolean match;
			try {
				match = predicate.test(t);
			}
			catch (Throwable e) {
				Context ctx = actual.currentContext();
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscardMultiple(buffer, ctx);
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (mode == Mode.UNTIL && match) {
				b.add(t);
				onNextNewBuffer();
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				onNextNewBuffer();
				b = buffer;
				b.add(t);
			}
			else if (mode == Mode.WHILE && !match) {
				onNextNewBuffer();
			}
			else {
				b.add(t);
			}

			if (fastpath) {
				return true;
			}

			boolean isNotExpectingFromSource = REQUESTED_FROM_SOURCE.decrementAndGet(this) == 0;
			boolean isStillExpectingBuffer = REQUESTED_BUFFERS.get(this) > 0;
			if (isNotExpectingFromSource && isStillExpectingBuffer
					&& REQUESTED_FROM_SOURCE.compareAndSet(this, 0, 1)) {
				return false; //explicitly mark as "needing more", either in attached conditional or onNext()
			}
			return true;
		}

		@Nullable
		C triggerNewBuffer() {
			C b = buffer;

			if (b.isEmpty()) {
				//emit nothing and we'll reuse the same buffer
				return null;
			}

			//we'll create a new buffer
			C c;

			try {
				c = Objects.requireNonNull(bufferSupplier.get(),
						"The bufferSupplier returned a null buffer");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, actual.currentContext()));
				return null;
			}

			buffer = c;
			return b;
		}

		private void onNextNewBuffer() {
			C b = triggerNewBuffer();
			if (b != null) {
				if (fastpath) {
					actual.onNext(b);
					return;
				}
				long r = REQUESTED_BUFFERS.getAndDecrement(this);
				if (r > 0) {
					actual.onNext(b);
					return;
				}
				cancel();
				actual.onError(Exceptions.failWithOverflow("Could not emit buffer due to lack of requests"));
			}
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			cleanup();
			Operators.onDiscardMultiple(buffer, actual.currentContext());
			buffer = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			cleanup();
			DrainUtils.postComplete(actual, this, REQUESTED_BUFFERS, this, this);
		}

		void cleanup() {
			// necessary cleanup if predicate contains a state
			if (predicate instanceof Disposable) {
				((Disposable) predicate).dispose();
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return getAsBoolean();
			if (key == Attr.CAPACITY) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requestedBuffers;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public boolean getAsBoolean() {
			return s == Operators.cancelledSubscription();
		}

		@Override
		public Iterator<C> iterator() {
			if (isEmpty()) {
				return Collections.emptyIterator();
			}
			return Collections.singleton(buffer).iterator();
		}

		@Override
		public boolean offer(C objects) {
			throw new IllegalArgumentException();
		}

		@Override
		@Nullable
		public C poll() {
			C b = buffer;
			if (b != null && !b.isEmpty()) {
				buffer = null;
				return b;
			}
			return null;
		}

		@Override
		@Nullable
		public C peek() {
			return buffer;
		}

		@Override
		public int size() {
			return buffer == null || buffer.isEmpty() ? 0 : 1;
		}

		@Override
		public String toString() {
			return "FluxBufferPredicate";
		}
	}

	static class ChangedPredicate<T, K> implements Predicate<T>, Disposable {

		private Function<? super T, ? extends K>  keySelector;
		private BiPredicate<? super K, ? super K> keyComparator;
		private K                                 lastKey;

		ChangedPredicate(Function<? super T, ? extends K> keySelector,
				BiPredicate<? super K, ? super K> keyComparator) {
			this.keySelector = keySelector;
			this.keyComparator = keyComparator;
		}

		@Override
		public void dispose() {
			lastKey = null;
		}

		@Override
		public boolean test(T t) {
			K k = keySelector.apply(t);

			if (null == lastKey) {
				lastKey = k;
				return false;
			}

			boolean match;
			match = keyComparator.test(lastKey, k);
			lastKey = k;

			return !match;
		}

	}
}

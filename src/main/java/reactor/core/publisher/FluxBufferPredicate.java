/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Trackable;

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
		extends FluxSource<T, C> {

	public enum Mode {
		UNTIL, UNTIL_CUT_BEFORE, WHILE
	}

	final Predicate<? super T> predicate;

	final Supplier<C> bufferSupplier;

	final Mode mode;

	public FluxBufferPredicate(Publisher<? extends T> source, Predicate<? super T> predicate,
			Supplier<C> bufferSupplier, Mode mode) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.mode = mode;
	}

	@Override
	public long getPrefetch() {
		return 1; //this operator changes the downstream request to 1 in the source
	}
	
	@Override
	public void subscribe(Subscriber<? super C> s) {
		C initialBuffer;
		
		try {
			initialBuffer = bufferSupplier.get();
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}
		
		if (initialBuffer == null) {
			Operators.error(s, new NullPointerException("The bufferSupplier returned a null initial buffer"));
			return;
		}

		BufferPredicateSubscriber<T, C> parent = new BufferPredicateSubscriber<>(s,
				initialBuffer, bufferSupplier, predicate, mode);

		source.subscribe(parent);
	}
	
	static final class BufferPredicateSubscriber<T, C extends Collection<? super T>>
			extends AbstractQueue<C>
			implements ConditionalSubscriber<T>, Subscription, Trackable,
			           BooleanSupplier {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final Mode mode;

		final Predicate<? super T> predicate;

		C buffer;

		boolean done;

		volatile boolean fastpath;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferPredicateSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferPredicateSubscriber.class, "requested");

		volatile Subscription s;

		static final AtomicReferenceFieldUpdater<BufferPredicateSubscriber,
				Subscription> S = AtomicReferenceFieldUpdater.newUpdater
				(BufferPredicateSubscriber.class, Subscription.class, "s");

		volatile long produced;


		BufferPredicateSubscriber(Subscriber<? super C> actual, C initialBuffer,
				Supplier<C> bufferSupplier, Predicate<? super T> predicate, Mode mode) {
			this.actual = actual;
			this.buffer = initialBuffer;
			this.bufferSupplier = bufferSupplier;
			this.predicate = predicate;
			this.mode = mode;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n) && !done) {
				long expectedRequested = Operators.addCap(n, requested);
				if (expectedRequested == Long.MAX_VALUE) {
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
					if (!DrainUtils.postCompleteRequest(n, actual, this, REQUESTED, this, this)) {
						s.request(n);
					}
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

			C b = buffer;
			boolean match;
			try {
				match = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			if (mode == Mode.UNTIL && match) {
				b.add(t);
				emitAndTriggerNewBuffer();
			}
			else if (mode == Mode.UNTIL_CUT_BEFORE && match) {
				emitAndTriggerNewBuffer();
				b = buffer;
				b.add(t);
			}
			else if (mode == Mode.WHILE && !match) {
				emitAndTriggerNewBuffer();
			}
			else {
				b.add(t);
				produced++;
				if (produced >= requested && !fastpath) {
					//already added what was block-requested at the beginning but still
					// not enough to fill the buffer, continue requesting from the source
					return false;
				}
			}
			return true;
		}

		private C triggerNewBuffer() {
			C b = buffer;
			if (done) {
				buffer = null;
				return b;
			}

			if (b.isEmpty()) {
				//emit nothing and we'll reuse the same buffer
				return null;
			}

			//we'll create a new buffer
			C c;

			try {
				c = bufferSupplier.get();
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e));
				return null;
			}

			if (c == null) {
				cancel();
				onError(new NullPointerException("The bufferSupplier returned a null buffer"));
				return null;
			}

			buffer = c;
			return b;
		}

		private void emitAndTriggerNewBuffer() {
			C b = triggerNewBuffer();
			if (b != null && emit(b)) {
				produced = 0; //reset the produced counter each time a new buffer is emitted
				long r = requested;
				if (r > 0 && !fastpath) {
					//when a buffer has been emitted, if more buffers request are pending then
					//request more data from source (with a potentially larger initial batch)
					s.request(r);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			buffer = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			long p = produced;
			if (p != 0L) {
				REQUESTED.decrementAndGet(this);
			}
			DrainUtils.postCompleteDrain(1, actual, this, REQUESTED, this, this);
		}

		boolean emit(C b) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(b);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			}
			else {
				cancel();
				actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));
				return false;
			}
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
			C b = buffer;
			return b != null ? b.size() : 0L;
		}

		@Override
		public boolean getAsBoolean() {
			return isCancelled();
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
		public C poll() {
			C b = buffer;
			if (b != null && !b.isEmpty()) {
				b = triggerNewBuffer();
				return b;
			}
			return null;
		}

		@Override
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
}

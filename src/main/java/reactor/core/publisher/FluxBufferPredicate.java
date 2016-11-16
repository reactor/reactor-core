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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
 *     non-matching element is simply discarded.</li>
 * </ul>
 *
 * @param <T> the source value type
 * @param <C> the output collection type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBufferPredicate<T, C extends Collection<? super T>>
		extends FluxSource<T, C> {

	public enum Mode {
		UNTIL, UNTIL_OTHER, WHILE;
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
		return Long.MAX_VALUE;
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

		s.onSubscribe(parent);//?
		source.subscribe(parent);
	}
	
	static final class BufferPredicateSubscriber<T, C extends Collection<? super T>>
	implements Subscriber<T>, Subscription {

		final Subscriber<? super C> actual;
		
		final Supplier<C> bufferSupplier;

		final Predicate<? super T> predicate;

		final Mode mode;

		C buffer;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BufferPredicateSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(BufferPredicateSubscriber.class, Subscription.class, "s");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferPredicateSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferPredicateSubscriber.class, "requested");
		
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
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			C b;
			synchronized (this) {
				b = buffer;
			}

			if (b == null) {
				Operators.onNextDropped(t);
			}
			else {
				boolean match;
				try {
					match = predicate.test(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}

				if (mode == Mode.UNTIL && match) {
					b.add(t);
					triggerNewBuffer();
				}
				else if (mode == Mode.UNTIL_OTHER && match) {
					triggerNewBuffer();
					synchronized (this) {
						b = buffer;
					}
					b.add(t);
				}
				else if (mode == Mode.WHILE && !match) {
					triggerNewBuffer();
				}
				else {
					b.add(t);
				}
			}
		}

		private void triggerNewBuffer() {
			C b;
			synchronized (this) {
				b = buffer;
			}

			if (b.isEmpty()) {
				return; //will reuse the same buffer
			}

			//we'll create a new buffer
			C c;

			try {
				c = bufferSupplier.get();
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e));
				return;
			}

			if (c == null) {
				cancel();
				onError(new NullPointerException("The bufferSupplier returned a null buffer"));
				return;
			}

			synchronized (this) {
				buffer = c;
			}

			emit(b);
		}

		@Override
		public void onError(Throwable t) {
			Subscription s = this.s;
			if (s != null) {
				this.s = null;
			}
			else {
				Operators.onErrorDropped(t);
			}
			boolean report;

			synchronized (this) {
				C b = buffer;

				if (b != null) {
					buffer = null;
					report = true;
				}
				else {
					report = false;
				}
			}

			if (report) {
				actual.onError(t);
			}
			else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			Subscription s = this.s;
			if (s != null) {
				this.s = null;
			}
			else {
				return;
			}

			C b;
			synchronized (this) {
				b = buffer;
				buffer = null;
			}

			if (b != null && !b.isEmpty()) {
				if (emit(b)) {
					actual.onComplete();
				}
			}
			else {
				actual.onComplete();
			}
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

				actual.onError(new IllegalStateException(
						"Could not emit buffer due to lack of requests"));

				return false;
			}
		}
	}
}

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

/**
 * Buffers elements into custom collections where the buffer boundary is signalled
 * by another publisher.
 *
 * @param <T> the source value type
 * @param <U> the element type of the boundary publisher (irrelevant)
 * @param <C> the output collection type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxBufferBoundary<T, U, C extends Collection<? super T>>
		extends FluxSource<T, C> {

	final Publisher<U> other;

	final Supplier<C> bufferSupplier;

	FluxBufferBoundary(Publisher<? extends T> source,
			Publisher<U> other,
			Supplier<C> bufferSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super C> s) {
		C buffer;

		try {
			buffer = bufferSupplier.get();
		}
		catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		if (buffer == null) {
			Operators.error(s,
					Operators.onOperatorError(new NullPointerException("The bufferSupplier returned a null buffer")));
			return;
		}

		BufferBoundaryMain<T, U, C> parent =
				new BufferBoundaryMain<>(
						source instanceof FluxInterval ? s : Operators.serialize(s),
						buffer, bufferSupplier);

		BufferBoundaryOther<U> boundary = new BufferBoundaryOther<>(parent);
		parent.other = boundary;

		s.onSubscribe(parent);

		other.subscribe(boundary);

		source.subscribe(parent);
	}

	static final class BufferBoundaryMain<T, U, C extends Collection<? super T>>
			implements Subscriber<T>, Subscription {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		BufferBoundaryOther<U> other;

		C buffer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BufferBoundaryMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(BufferBoundaryMain.class,
						Subscription.class,
						"s");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferBoundaryMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferBoundaryMain.class, "requested");

		BufferBoundaryMain(Subscriber<? super C> actual,
				C buffer,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.buffer = buffer;
			this.bufferSupplier = bufferSupplier;
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
			other.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (this) {
				C b = buffer;
				if (b != null) {
					b.add(t);
					return;
				}
			}

			Operators.onNextDropped(t);
		}

		@Override
		public void onError(Throwable t) {
			if(Operators.setTerminated(S, this)) {
				synchronized (this) {
					buffer = null;
				}

				other.cancel();
				actual.onError(t);
				return;
			}
			Operators.onErrorDropped(t);
		}

		@Override
		public void onComplete() {
			if(Operators.setTerminated(S, this)) {
				C b;
				synchronized (this) {
					b = buffer;
					buffer = null;
				}

				other.cancel();
				if (!b.isEmpty()) {
					if (emit(b)) {
						actual.onComplete();
					}
				}
				else {
					actual.onComplete();
				}
			}
		}
		void otherComplete() {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if(s != Operators.cancelledSubscription()) {
				C b;
				synchronized (this) {
					b = buffer;
					buffer = null;
				}

				if(s != null){
					s.cancel();
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
		}

		void otherError(Throwable t){
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if(s != Operators.cancelledSubscription()) {
				synchronized (this) {
					buffer = null;
				}

				if(s != null){
					s.cancel();
				}

				actual.onError(t);
				return;
			}
			Operators.onErrorDropped(t);
		}
		void otherNext() {
			C c;

			try {
				c = bufferSupplier.get();
			}
			catch (Throwable e) {
				otherError(Operators.onOperatorError(other, e));
				return;
			}

			if (c == null) {
				otherError(Operators.onOperatorError(other, new NullPointerException("The bufferSupplier returned a null buffer")));
				return;
			}

			C b;
			synchronized (this) {
				b = buffer;
				buffer = c;
			}

			if (b == null || b.isEmpty()) {
				return;
			}

			emit(b);
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
				actual.onError(Operators.onOperatorError(this, Exceptions
						.failWithOverflow(), b));

				return false;
			}
		}
	}

	static final class BufferBoundaryOther<U> extends Operators.DeferredSubscription
			implements Subscriber<U> {

		final BufferBoundaryMain<?, U, ?> main;

		BufferBoundaryOther(BufferBoundaryMain<?, U, ?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			main.otherNext();
		}

		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}

		@Override
		public void onComplete() {
			main.otherComplete();
		}
	}
}

/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

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
		extends InternalFluxOperator<T, C> {

	final Publisher<U> other;

	final Supplier<C> bufferSupplier;

	FluxBufferBoundary(Flux<? extends T> source,
			Publisher<U> other,
			Supplier<C> bufferSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		C buffer = Objects.requireNonNull(bufferSupplier.get(),
				"The bufferSupplier returned a null buffer");

		BufferBoundaryMain<T, U, C> parent =
				new BufferBoundaryMain<>(
						source instanceof FluxInterval ?
								actual : Operators.serialize(actual),
						buffer, bufferSupplier);

		actual.onSubscribe(parent);

		other.subscribe(parent.other);

		return parent;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == RUN_STYLE) return SYNC;
		return super.scanUnsafe(key);
	}

	static final class BufferBoundaryMain<T, U, C extends Collection<? super T>>
			implements InnerOperator<T, C> {

		final Supplier<C>           bufferSupplier;
		final CoreSubscriber<? super C> actual;
		final Context ctx;

		final BufferBoundaryOther<U> other;

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

		BufferBoundaryMain(CoreSubscriber<? super C> actual,
				C buffer,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.ctx =  actual.currentContext();
			this.buffer = buffer;
			this.bufferSupplier = bufferSupplier;
			this.other = new BufferBoundaryOther<>(this);
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.CAPACITY) {
				C buffer = this.buffer;
				return buffer != null ? buffer.size() : 0;
			}
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == RUN_STYLE) return SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
			Operators.onDiscardMultiple(buffer, this.ctx);
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

			Operators.onNextDropped(t, this.ctx);
		}

		@Override
		public void onError(Throwable t) {
			if(Operators.terminate(S, this)) {
				C b;
				synchronized (this) {
					b = buffer;
					buffer = null;
				}

				other.cancel();
				actual.onError(t);
				Operators.onDiscardMultiple(b, this.ctx);
				return;
			}
			Operators.onErrorDropped(t, this.ctx);
		}

		@Override
		public void onComplete() {
			if(Operators.terminate(S, this)) {
				C b;
				synchronized (this) {
					b = buffer;
					buffer = null;
				}

				other.cancel();
				if (!b.isEmpty()) {
					if (emit(b)) {
						actual.onComplete();
					} //failed emit will discard buffer's elements
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
					} //failed emit will discard buffer content
				}
				else {
					actual.onComplete();
				}
			}
		}

		void otherError(Throwable t){
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

				actual.onError(t);
				Operators.onDiscardMultiple(b, this.ctx);
				return;
			}
			Operators.onErrorDropped(t, this.ctx);
		}

		void otherNext() {
			C c;

			try {
				c = Objects.requireNonNull(bufferSupplier.get(),
						"The bufferSupplier returned a null buffer");
			}
			catch (Throwable e) {
				otherError(Operators.onOperatorError(other, e, this.ctx));
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
						.failWithOverflow(), b, this.ctx));
				Operators.onDiscardMultiple(b, this.ctx);
				return false;
			}
		}
	}

	static final class BufferBoundaryOther<U> extends Operators.DeferredSubscription
			implements InnerConsumer<U> {

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
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return main;
			}
			if (key == RUN_STYLE) {
			    return SYNC;
			}
			return super.scanUnsafe(key);
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

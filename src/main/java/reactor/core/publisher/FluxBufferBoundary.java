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
import reactor.core.subscriber.SubscriptionHelper;
import reactor.core.subscriber.DeferredSubscription;
import reactor.core.util.Exceptions;

/**
 * Buffers elements into custom collections where the buffer boundary is signalled
 * by another publisher.
 *
 * @param <T> the source value type
 * @param <U> the element type of the boundary publisher (irrelevant)
 * @param <C> the output collection type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxBufferBoundary<T, U, C extends Collection<? super T>>
		extends FluxSource<T, C> {

	final Publisher<U> other;
	
	final Supplier<C> bufferSupplier;

	public FluxBufferBoundary(Publisher<? extends T> source,
			Publisher<U> other, Supplier<C> bufferSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}
	
	@Override
	public void subscribe(Subscriber<? super C> s) {
		C buffer;
		
		try {
			buffer = bufferSupplier.get();
		} catch (Throwable e) {
			SubscriptionHelper.error(s, e);
			return;
		}
		
		if (buffer == null) {
			SubscriptionHelper.error(s, new NullPointerException("The bufferSupplier returned a null buffer"));
			return;
		}
		
		BufferBoundaryMain<T, U, C> parent = new BufferBoundaryMain<>(s, buffer, bufferSupplier);
		
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
				AtomicReferenceFieldUpdater.newUpdater(BufferBoundaryMain.class, Subscription.class, "s");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BufferBoundaryMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferBoundaryMain.class, "requested");
		
		public BufferBoundaryMain(Subscriber<? super C> actual, C buffer, Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.buffer = buffer;
			this.bufferSupplier = bufferSupplier;
		}
		
		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				SubscriptionHelper.getAndAddCap(REQUESTED, this, n);
			}
		}

		void cancelMain() {
			SubscriptionHelper.terminate(S, this);
		}
		
		@Override
		public void cancel() {
			cancelMain();
			other.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.setOnce(S, this, s)) {
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
			
			Exceptions.onNextDropped(t);
		}

		@Override
		public void onError(Throwable t) {
			boolean report;
			synchronized (this) {
				C b = buffer;
				
				if (b != null) {
					buffer = null;
					report = true;
				} else {
					report = false;
				}
			}
			
			if (report) {
				other.cancel();
				
				actual.onError(t);
			} else {
				Exceptions.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
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
		}
		
		void otherNext() {
			C c;
			
			try {
				c = bufferSupplier.get();
			} catch (Throwable e) {
				other.cancel();
				
				otherError(e);
				return;
			}
			
			if (c == null) {
				other.cancel();

				otherError(new NullPointerException("The bufferSupplier returned a null buffer"));
				return;
			}
			
			C b;
			synchronized (this) {
				b = buffer;
				if (b == null) {
					return;
				}
				buffer = c;
			}
			
			emit(b);
		}
		
		void otherError(Throwable e) {
			cancelMain();
			
			onError(e);
		}
		
		void otherComplete() {
			cancelMain();

			onComplete();
		}
		
		boolean emit(C b) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(b);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			} else {
				cancel();
				
				actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));

				return false;
			}
		}
	}
	
	static final class BufferBoundaryOther<U> extends DeferredSubscription
	implements Subscriber<U> {
		
		final BufferBoundaryMain<?, U, ?> main;
		
		public BufferBoundaryOther(BufferBoundaryMain<?, U, ?> main) {
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

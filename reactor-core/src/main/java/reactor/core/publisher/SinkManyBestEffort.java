/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class SinkManyBestEffort<T> extends Flux<T>
		implements InternalManySink<T>, Scannable,
		           DirectInnerContainer<T> {

	static final DirectInner[] EMPTY      = new DirectInner[0];
	static final DirectInner[] TERMINATED = new DirectInner[0];

	static final <T> SinkManyBestEffort<T> createBestEffort() {
		return new SinkManyBestEffort<>(false);
	}

	static final <T> SinkManyBestEffort<T> createAllOrNothing() {
		return new SinkManyBestEffort<>(true);
	}

	final boolean allOrNothing;

	/**
	 * Stores the error that terminated this sink, for immediate replay to late subscribers
	 */
	Throwable error;

	volatile     DirectInner<T>[]                                              subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkManyBestEffort, DirectInner[]>SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(SinkManyBestEffort.class, DirectInner[].class, "subscribers");

	SinkManyBestEffort(boolean allOrNothing) {
		this.allOrNothing = allOrNothing;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return subscribers == TERMINATED;
		if (key == Attr.ERROR) return error;

		return null;
	}

	@Override
	public EmitResult tryEmitNext(T t) {
		Objects.requireNonNull(t, "tryEmitNext(null) is forbidden");

		DirectInner<T>[] subs = subscribers;

		if (subs == EMPTY) {
			return EmitResult.FAIL_ZERO_SUBSCRIBER;
		}
		if (subs == TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		int expectedEmitted = subs.length;
		int cancelledCount = 0;
		if (allOrNothing) {
			long commonRequest = Long.MAX_VALUE;
			for (DirectInner<T> sub : subs) {
				long subRequest = sub.requested;
				if (sub.isCancelled()) {
					cancelledCount++;
					continue;
				}
				if (subRequest < commonRequest) {
					commonRequest = subRequest;
				}
			}
			if (commonRequest == 0) {
				return EmitResult.FAIL_OVERFLOW;
			}
			if (cancelledCount == expectedEmitted) {
				return EmitResult.FAIL_ZERO_SUBSCRIBER;
			}
		}

		int emittedCount = 0;
		cancelledCount = 0;
		for (DirectInner<T> sub : subs) {
			if (sub.isCancelled()) {
				cancelledCount++;
				continue;
			}
			if (sub.tryEmitNext(t)) {
				emittedCount++;
			}
			else if (sub.isCancelled()) {
				cancelledCount++;
			}
		}
		if (cancelledCount == expectedEmitted) {
			return EmitResult.FAIL_ZERO_SUBSCRIBER;
		}
		else if (cancelledCount + emittedCount == expectedEmitted) {
			return EmitResult.OK;
		}
		else if (emittedCount > 0 && !allOrNothing) {
			return EmitResult.OK;
		}
		else {
			return EmitResult.FAIL_OVERFLOW;
		}
	}

	@Override
	public EmitResult tryEmitComplete() {
		@SuppressWarnings("unchecked")
		DirectInner<T>[] subs = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (subs == TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		for (DirectInner<?> s : subs) {
			s.emitComplete();
		}
		return EmitResult.OK;
	}

	@Override
	public EmitResult tryEmitError(Throwable error) {
		Objects.requireNonNull(error, "tryEmitError(null) is forbidden");
		@SuppressWarnings("unchecked")
		DirectInner<T>[] subs = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (subs == TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		this.error = error;

		for (DirectInner<?> s : subs) {
			s.emitError(error);
		}
		return EmitResult.OK;
	}

	@Override
	public int currentSubscriberCount() {
		return subscribers.length;
	}

	@Override
	public Flux<T> asFlux() {
		return this;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe(null) is forbidden");

		DirectInner<T> p = new DirectInner<>(actual, this);
		actual.onSubscribe(p);

		if (p.isCancelled()) {
			return;
		}

		if (add(p)) {
			if (p.isCancelled()) {
				remove(p);
			}
		}
		else {
			Throwable e = error;
			if (e != null) {
				actual.onError(e);
			}
			else {
				actual.onComplete();
			}
		}
	}

	@Override
	public boolean add(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked")
			DirectInner<T>[] b = new DirectInner[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void remove(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == TERMINATED || a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int len = a.length;

			int j = -1;

			for (int i = 0; i < len; i++) {
				if (a[i] == s) {
					j = i;
					break;
				}
			}
			if (j < 0) {
				return;
			}
			if (len == 1) {
				subscribers = EMPTY;
				return;
			}

			DirectInner<T>[] b = new DirectInner[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	static class DirectInner<T> extends AtomicBoolean implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final DirectInnerContainer<T>   parent;

		volatile     long                                requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DirectInner> REQUESTED = AtomicLongFieldUpdater.newUpdater(
				DirectInner.class, "requested");

		DirectInner(CoreSubscriber<? super T> actual, DirectInnerContainer<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (compareAndSet(false, true)) {
				parent.remove(this);
			}
		}

		boolean isCancelled() {
			return get();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.CANCELLED) return isCancelled();

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * Try to emit if the downstream is not cancelled and has some demand.
		 * @param value the value to emit
		 * @return true if enough demand and not cancelled, false otherwise
		 */
		boolean tryEmitNext(T value) {
			if (requested != 0L) {
				if (isCancelled()) {
					return false;
				}
				actual.onNext(value);
				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			}
			return false;
		}

		/**
		 * Emit a value to the downstream, unless it doesn't have sufficient demand.
		 * In that case, the downstream is terminated with an {@link Exceptions#failWithOverflow()}.
		 *
		 * @param value the value to emit
		 */
		void directEmitNext(T value) {
			if (requested != 0L) {
				actual.onNext(value);
				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return;
			}
			parent.remove(this);
			actual.onError(Exceptions.failWithOverflow("Can't deliver value due to lack of requests"));
		}

		void emitError(Throwable e) {
			if (isCancelled()) {
				return;
			}
			actual.onError(e);
		}

		void emitComplete() {
			if (isCancelled()) {
				return;
			}
			actual.onComplete();
		}
	}

}


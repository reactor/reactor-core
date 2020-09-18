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
import reactor.core.publisher.Sinks.Emission;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class SinkManyBestEffort<T> extends Flux<T> implements Sinks.Many<T>, ContextHolder,
                                                             Scannable {

	private static final Inner[] EMPTY = new Inner[0];
	private static final Inner[] TERMINATED = new Inner[0];

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

	volatile     Inner<T>[]                                               subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkManyBestEffort, Inner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(SinkManyBestEffort.class, Inner[].class, "subscribers");

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
	public Emission tryEmitNext(T t) {
		Objects.requireNonNull(t, "tryEmitNext(null) is forbidden");

		Inner<T>[] subs = subscribers;

		if (subs == EMPTY) {
			return Emission.FAIL_ZERO_SUBSCRIBER;
		}
		if (subs == TERMINATED) {
			return Emission.FAIL_TERMINATED;
		}

		if (allOrNothing) {
			long commonRequest = Long.MAX_VALUE;
			for (Inner<T> sub : subs) {
				long subRequest = sub.requested;
				if (sub.isCancelled()) {
					continue;
				}
				if (subRequest < commonRequest) {
					commonRequest = subRequest;
				}
			}
			if (commonRequest == 0) {
				return Emission.FAIL_OVERFLOW;
			}
		}

		int expectedEmitted = subs.length;
		int emittedCount = 0;
		int cancelledCount = 0;
		for (Inner<T> sub : subs) {
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
			return Emission.FAIL_ZERO_SUBSCRIBER; //TODO should it be FAIL_CANCELLED ? but technically the sink would still be usable
		}
		else if (cancelledCount + emittedCount == expectedEmitted) {
			return Emission.OK;
		}
		else {
			return Emission.FAIL_OVERFLOW;
		}
	}

	@Override
	public Emission tryEmitComplete() {
		@SuppressWarnings("unchecked")
		Inner<T>[] subs = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (subs == TERMINATED) {
			return Emission.FAIL_TERMINATED;
		}

		for (Inner<?> s : subs) {
			s.emitComplete();
		}
		return Emission.OK;
	}

	@Override
	public Emission tryEmitError(Throwable error) {
		Objects.requireNonNull(error, "tryEmitError(null) is forbidden");
		@SuppressWarnings("unchecked")
		Inner<T>[] subs = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (subs == TERMINATED) {
			return Emission.FAIL_TERMINATED;
		}

		this.error = error;

		for (Inner<?> s : subs) {
			s.emitError(error);
		}
		return Emission.OK;
	}

	@Override
	public void emitComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused")
		Emission emission = tryEmitComplete();
	}

	@Override
	public void emitError(Throwable error) {
		Emission result = tryEmitError(error);
		if (result == Emission.FAIL_TERMINATED) {
			Operators.onErrorDroppedMulticast(error, subscribers);
		}
	}

	@Override
	public void emitNext(T value) {
		switch(tryEmitNext(value)) {
			case FAIL_ZERO_SUBSCRIBER:
				//we want to "discard" without rendering the sink terminated.
				// effectively NO-OP cause there's no subscriber, so no context :(
				break;
			case FAIL_OVERFLOW:
				Operators.onDiscard(value, currentContext());
				//the emitError will onErrorDropped if already terminated
				emitError(Exceptions.failWithOverflow("Backpressure overflow during Sinks.Many#emitNext"));
				break;
			case FAIL_CANCELLED:
				Operators.onDiscard(value, currentContext());
				break;
			case FAIL_TERMINATED:
				Operators.onNextDroppedMulticast(value, subscribers);
				break;
			case OK:
				break;
			case FAIL_NON_SERIALIZED:
				throw new IllegalStateException("Unexpected return code");
		}
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

		Inner<T> p = new Inner<>(actual, this);
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

	boolean add(Inner<T> s) {
		Inner<T>[] a = subscribers;
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
			Inner<T>[] b = new Inner[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(Inner<T> s) {
		Inner<T>[] a = subscribers;
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

			Inner<T>[] b = new Inner[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	static class Inner<T> extends AtomicBoolean implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final SinkManyBestEffort<T>     parent;

		volatile     long                          requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<Inner> REQUESTED = AtomicLongFieldUpdater.newUpdater(Inner.class, "requested");

		Inner(CoreSubscriber<? super T> actual, SinkManyBestEffort<T> parent) {
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

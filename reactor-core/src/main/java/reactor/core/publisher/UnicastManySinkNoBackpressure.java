/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class UnicastManySinkNoBackpressure<T> extends Flux<T> implements InternalManySink<T>, Subscription, ContextHolder {

	public static <E> UnicastManySinkNoBackpressure<E> create() {
		return new UnicastManySinkNoBackpressure<>();
	}

	enum State {
		INITIAL,
		SUBSCRIBED,
		TERMINATED,
		CANCELLED,
	}

	volatile State state;

	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<UnicastManySinkNoBackpressure, State> STATE = AtomicReferenceFieldUpdater.newUpdater(
			UnicastManySinkNoBackpressure.class,
			State.class,
			"state"
	);

	private volatile CoreSubscriber<? super T> actual = null;

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<UnicastManySinkNoBackpressure> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(UnicastManySinkNoBackpressure.class, "requested");

	UnicastManySinkNoBackpressure() {
		STATE.lazySet(this, State.INITIAL);
	}

	@Override
	public int currentSubscriberCount() {
		return state == State.SUBSCRIBED ? 1 : 0;
	}

	@Override
	public Flux<T> asFlux() {
		return this;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");

		if (!STATE.compareAndSet(this, State.INITIAL, State.SUBSCRIBED)) {
			Operators.reportThrowInSubscribe(actual, new IllegalStateException("Unicast Sinks.Many allows only a single Subscriber"));
			return;
		}

		this.actual = actual;
		actual.onSubscribe(this);
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
		}
	}

	@Override
	public void cancel() {
		if (STATE.getAndSet(this, State.CANCELLED) == State.SUBSCRIBED) {
			actual = null;
		}
	}

	@Override
	public Context currentContext() {
		CoreSubscriber<? super T> actual = this.actual;
		return actual != null ? actual.currentContext() : Context.empty();
	}

	@Override
	public Sinks.EmitResult tryEmitNext(T t) {
		Objects.requireNonNull(t, "t");

		switch (state) {
			case INITIAL:
				return Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
			case SUBSCRIBED:
				if (requested == 0L) {
					return Sinks.EmitResult.FAIL_OVERFLOW;
				}

				actual.onNext(t);
				Operators.produced(REQUESTED, this, 1);
				return Sinks.EmitResult.OK;
			case TERMINATED:
				return Sinks.EmitResult.FAIL_TERMINATED;
			case CANCELLED:
				return Sinks.EmitResult.FAIL_CANCELLED;
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public EmitResult tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t");
		for(;;) { //for the benefit of retrying SUBSCRIBED
			State s = this.state;
			switch (s) {
				case INITIAL:
					return Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
				case SUBSCRIBED:
					if (STATE.compareAndSet(this, s, State.TERMINATED)) {
						actual.onError(t);
						actual = null;
						return Sinks.EmitResult.OK;
					}
					continue;
				case TERMINATED:
					return Sinks.EmitResult.FAIL_TERMINATED;
				case CANCELLED:
					return Sinks.EmitResult.FAIL_CANCELLED;
				default:
					throw new IllegalStateException();
			}
		}
	}

	@Override
	public EmitResult tryEmitComplete() {
		for (;;) { //for the benefit of retrying SUBSCRIBED
			State s = this.state;
			switch (s) {
				case INITIAL:
					return Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
				case SUBSCRIBED:
					if (STATE.compareAndSet(this, s, State.TERMINATED)) {
						actual.onComplete();
						actual = null;
						return EmitResult.OK;
					}
					continue;
				case TERMINATED:
					return Sinks.EmitResult.FAIL_TERMINATED;
				case CANCELLED:
					return Sinks.EmitResult.FAIL_CANCELLED;
				default:
					throw new IllegalStateException();
			}
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		CoreSubscriber<? super T> a = actual;
		return a == null ? Stream.empty() : Stream.of(Scannable.from(a));
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ACTUAL) return actual;
		if (key == Attr.TERMINATED) return state == State.TERMINATED;
		if (key == Attr.CANCELLED) return state == State.CANCELLED;

		return null;
	}
}

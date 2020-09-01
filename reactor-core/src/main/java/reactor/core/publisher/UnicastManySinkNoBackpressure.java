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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.Emission;
import reactor.util.context.Context;

final class UnicastManySinkNoBackpressure<T> extends Flux<T> implements Sinks.Many<T>, Subscription, ContextHolder {

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
		switch (STATE.getAndSet(this, State.CANCELLED)) {
			case SUBSCRIBED:
				actual = null;
				break;
		}
	}

	@Override
	public Context currentContext() {
		CoreSubscriber<? super T> actual = this.actual;
		return actual != null ? actual.currentContext() : Context.empty();
	}

	@Override
	public void emitNext(T value) {
		switch (tryEmitNext(value)) {
			case FAIL_OVERFLOW:
				Operators.onDiscard(value, currentContext());
				//the emitError will onErrorDropped if already terminated
				emitError(Exceptions.failWithOverflow("Backpressure overflow during Sinks.Many#emitNext"));
				break;
			case FAIL_CANCELLED:
				Operators.onDiscard(value, currentContext());
				break;
			case FAIL_TERMINATED:
				Operators.onNextDropped(value, currentContext());
				break;
			case OK:
				break;
		}
	}

	@Override
	public Emission tryEmitNext(T t) {
		Objects.requireNonNull(t, "t");

		switch (state) {
			case INITIAL:
				// TODO different Emission?
				return Emission.FAIL_OVERFLOW;
			case SUBSCRIBED:
				if (requested == 0L) {
					return Emission.FAIL_OVERFLOW;
				}

				actual.onNext(t);
				Operators.produced(REQUESTED, this, 1);
				return Emission.OK;
			case TERMINATED:
				return Emission.FAIL_TERMINATED;
			case CANCELLED:
				return Emission.FAIL_CANCELLED;
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public void emitError(Throwable error) {
		Emission result = tryEmitError(error);
		if (result == Emission.FAIL_TERMINATED) {
			Operators.onErrorDropped(error, currentContext());
		}
	}

	@Override
	public Emission tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t");

		switch (state) {
			case INITIAL:
				// TODO different Emission?
				return Emission.OK;
			case SUBSCRIBED:
				actual.onError(t);
				actual = null;
				return Emission.OK;
			case TERMINATED:
				return Emission.FAIL_TERMINATED;
			case CANCELLED:
				return Emission.FAIL_CANCELLED;
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public void emitComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused")
		Emission emission = tryEmitComplete();
	}

	@Override
	public Emission tryEmitComplete() {
		switch (state) {
			case INITIAL:
				// TODO different Emission?
				return Emission.OK;
			case SUBSCRIBED:
				actual.onComplete();
				actual = null;
				return Emission.OK;
			case TERMINATED:
				return Emission.FAIL_TERMINATED;
			case CANCELLED:
				return Emission.FAIL_CANCELLED;
			default:
				throw new IllegalStateException();
		}
	}
}

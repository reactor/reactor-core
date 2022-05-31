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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.SinkManyBestEffort.DirectInner;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Dispatches onNext, onError and onComplete signals to zero-to-many Subscribers.
 * Please note, that along with multiple consumers, current implementation of
 * DirectProcessor supports multiple producers. However, all producers must produce
 * messages on the same Thread, otherwise
 * <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a> contract is
 * violated.
 * <p>
 *      <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessornormal.png" alt="">
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *     <b>Note: </b> DirectProcessor does not coordinate backpressure between its
 *     Subscribers and the upstream, but consumes its upstream in an
 *     unbounded manner.
 *     In the case where a downstream Subscriber is not ready to receive items (hasn't
 *     requested yet or enough), it will be terminated with an
 *     <i>{@link IllegalStateException}</i>.
 *     Hence in terms of interaction model, DirectProcessor only supports PUSH from the
 *     source through the processor to the Subscribers.
 *
 *     <p>
 *        <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessorerror.png" alt="">
 *     </p>
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> If there are no Subscribers, upstream items are dropped and only
 *      the terminal events are retained. A terminated DirectProcessor will emit the
 *      terminal signal to late subscribers.
 *
 *      <p>
 *         <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessorterminal.png" alt="">
 *      </p>
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> The implementation ignores Subscriptions set via onSubscribe.
 * </p>
 *
 * @param <T> the input and output value type
 * @deprecated To be removed in 3.5, prefer clear cut usage of {@link Sinks}. Closest sink
 * is {@link Sinks.MulticastSpec#directBestEffort() Sinks.many().multicast().directBestEffort()},
 * except it doesn't terminate overflowing downstreams.
 */
@Deprecated
public final class DirectProcessor<T> extends FluxProcessor<T, T>
		implements DirectInnerContainer<T> {

	/**
	 * Create a new {@link DirectProcessor}
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 * @deprecated To be removed in 3.5. Closest sink is {@link Sinks.MulticastSpec#directBestEffort() Sinks.many().multicast().directBestEffort()},
	 * except it doesn't terminate overflowing downstreams.
	 */
	@Deprecated
	public static <E> DirectProcessor<E> create() {
		return new DirectProcessor<>();
	}

	@SuppressWarnings("unchecked")
	private volatile     DirectInner<T>[]                                           subscribers = SinkManyBestEffort.EMPTY;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<DirectProcessor, DirectInner[]>SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(DirectProcessor.class, DirectInner[].class, "subscribers");

	Throwable error;

	DirectProcessor() {
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	@Override
	public void onSubscribe(Subscription s) {
		Objects.requireNonNull(s, "s");
		if (subscribers != SinkManyBestEffort.TERMINATED) {
			s.request(Long.MAX_VALUE);
		}
		else {
			s.cancel();
		}
	}

	@Override
	public void onComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") Sinks.EmitResult emitResult = tryEmitComplete();
	}

	private void emitComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") EmitResult emitResult = tryEmitComplete();
	}

	private EmitResult tryEmitComplete() {
		@SuppressWarnings("unchecked")
		DirectInner<T>[] inners = SUBSCRIBERS.getAndSet(this, SinkManyBestEffort.TERMINATED);

		if (inners == SinkManyBestEffort.TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		for (DirectInner<?> s : inners) {
			s.emitComplete();
		}
		return EmitResult.OK;
	}

	@Override
	public void onError(Throwable throwable) {
		emitError(throwable);
	}

	private void emitError(Throwable error) {
		Sinks.EmitResult result = tryEmitError(error);
		if (result == EmitResult.FAIL_TERMINATED) {
			Operators.onErrorDroppedMulticast(error, subscribers);
		}
	}

	private Sinks.EmitResult tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t");

		@SuppressWarnings("unchecked")
		DirectInner<T>[] inners = SUBSCRIBERS.getAndSet(this, SinkManyBestEffort.TERMINATED);

		if (inners == SinkManyBestEffort.TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		error = t;
		for (DirectInner<?> s : inners) {
			s.emitError(t);
		}
		return Sinks.EmitResult.OK;
	}

	private void emitNext(T value) {
		switch (tryEmitNext(value)) {
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
			default:
				throw new IllegalStateException("unexpected return code");
		}
	}

	@Override
	public void onNext(T t) {
		emitNext(t);
	}

	private EmitResult tryEmitNext(T t) {
		Objects.requireNonNull(t, "t");

		DirectInner<T>[] inners = subscribers;

		if (inners == SinkManyBestEffort.TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (inners == SinkManyBestEffort.EMPTY) {
			return Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
		}

		for (DirectInner<T> s : inners) {
			s.directEmitNext(t);
		}
		return Sinks.EmitResult.OK;
	}

	@Override
	protected boolean isIdentityProcessor() {
		return true;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");

		DirectInner<T> p = new DirectInner<>(actual, this);
		actual.onSubscribe(p);

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
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public boolean isTerminated() {
		return SinkManyBestEffort.TERMINATED == subscribers;
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	@Override
	public boolean add(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == SinkManyBestEffort.TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == SinkManyBestEffort.TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") DirectInner<T>[] b = new DirectInner[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void remove(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == SinkManyBestEffort.TERMINATED || a == SinkManyBestEffort.EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == SinkManyBestEffort.TERMINATED || a == SinkManyBestEffort.EMPTY) {
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
				subscribers = SinkManyBestEffort.EMPTY;
				return;
			}

			DirectInner<T>[] b = new DirectInner[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	@Override
	public boolean hasDownstreams() {
		DirectInner<T>[] s = subscribers;
		return s != SinkManyBestEffort.EMPTY && s != SinkManyBestEffort.TERMINATED;
	}

	@Override
	@Nullable
	public Throwable getError() {
		if (subscribers == SinkManyBestEffort.TERMINATED) {
			return error;
		}
		return null;
	}

}

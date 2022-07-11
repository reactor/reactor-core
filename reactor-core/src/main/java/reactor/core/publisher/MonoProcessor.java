/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@code MonoProcessor} is a {@link Processor} that is also a {@link Mono}.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/monoprocessor.png" alt="">
 *
 * <p>
 * Implementations might implements stateful semantics, allowing multiple subscriptions.
 * Once a {@link MonoProcessor} has been resolved, implementations may also replay cached signals to newer subscribers.
 * <p>
 * Despite having default implementations, most methods should be reimplemented with meaningful semantics relevant to
 * concrete child classes.
 *
 * @param <O> the type of the value that will be made available
 *
 * @author Stephane Maldini
 * @deprecated Processors will be removed in 3.5. Prefer using {@link Sinks.One} or {@link Sinks.Empty} instead,
 * or see https://github.com/reactor/reactor-core/issues/2431 for alternatives
 */
@Deprecated
public abstract class MonoProcessor<O> extends Mono<O>
		implements Processor<O, O>, CoreSubscriber<O>, Disposable,
		           Subscription,
		           Scannable {

	/**
	 * Create a {@link MonoProcessor} that will eagerly request 1 on {@link #onSubscribe(Subscription)}, cache and emit
	 * the eventual result for 1 or N subscribers.
	 *
	 * @param <T> type of the expected value
	 *
	 * @return A {@link MonoProcessor}.
	 * @deprecated Use {@link Sinks#one()}, to be removed in 3.5
	 */
	@Deprecated
	public static <T> MonoProcessor<T> create() {
		return new NextProcessor<>(null);
	}

	/**
	 * @deprecated the {@link MonoProcessor} will cease to implement {@link Subscription} in 3.5
	 */
	@Override
	@Deprecated
	public void cancel() {
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been interrupted via cancellation.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is cancelled, {@code false}
	 * otherwise.
	 * @deprecated the {@link MonoProcessor} will cease to implement {@link Subscription} and this method will be removed in 3.5
	 */
	@Deprecated
	public boolean isCancelled() {
		return false;
	}

	/**
	 * @param n the request amount
	 * @deprecated the {@link MonoProcessor} will cease to implement {@link Subscription} in 3.5
	 */
	@Override
	@Deprecated
	public void request(long n) {
		Operators.validate(n);
	}

	@Override
	public void dispose() {
		onError(new CancellationException("Disposed"));
	}

	/**
	 * Block the calling thread indefinitely, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code MonoProcessor}
	 */
	@Override
	@Nullable
	public O block() {
		return block(null);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value as a {@link Duration}
	 *
	 * @return the value of this {@code MonoProcessor} or {@code null} if the timeout is reached and the {@code MonoProcessor} has
	 * not completed
	 */
	@Override
	@Nullable
	public O block(@Nullable Duration timeout) {
		return peek();
	}

	/**
	 * Return the produced {@link Throwable} error if any or null
	 *
	 * @return the produced {@link Throwable} error if any or null
	 */
	@Nullable
	public Throwable getError() {
		return null;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been completed with an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} was completed with an error, {@code false} otherwise.
	 */
	public final boolean isError() {
		return getError() != null;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public final boolean isSuccess() {
		return isTerminated() && !isError();
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been terminated by the
	 * source producer with a success or an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public boolean isTerminated() {
		return false;
	}

	@Override
	public boolean isDisposed() {
		return isTerminated() || isCancelled();
	}

	/**
	 * Returns the value that completed this {@link MonoProcessor}. Returns {@code null} if the {@link MonoProcessor} has not been completed. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the {@link MonoProcessor}, or {@code null} if it has not been completed
	 *
	 * @throws RuntimeException if the {@link MonoProcessor} was completed with an error
	 * @deprecated this method is discouraged, consider peeking into a MonoProcessor by {@link Mono#toFuture() turning it into a CompletableFuture}
	 */
	@Nullable
	@Deprecated
	public O peek() {
		return null;
	}

	@Override
	public Context currentContext() {
		InnerProducer<?>[] innerProducersArray =
				inners().filter(InnerProducer.class::isInstance)
				        .map(InnerProducer.class::cast)
				        .toArray(InnerProducer[]::new);

		return Operators.multiSubscribersContext(innerProducersArray);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		//touch guard
		boolean t = isTerminated();

		if (key == Attr.TERMINATED) return t;
		if (key == Attr.ERROR) return getError();
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
		if (key == Attr.CANCELLED) return isCancelled();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	public long downstreamCount() {
		return inners().count();
	}

	/**
	 * Return true if any {@link Subscriber} is actively subscribed
	 *
	 * @return true if any {@link Subscriber} is actively subscribed
	 */
	public final boolean hasDownstreams() {
		return downstreamCount() != 0;
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.empty();
	}
}

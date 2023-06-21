/*
 * Copyright (c) 2015-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Wrapper API around an actual downstream Subscriber
 * for emitting nothing, a single value or an error (mutually exclusive).
 *
 * @param <T> the value type emitted
 */
public interface MonoSink<T> {

	/**
	 * Complete without any value. <p>Calling this method multiple times or after the
	 * other terminating methods has no effect.
	 */
	void success();

	/**
	 * Complete this {@link Mono} with the given value.
	 * <p>Calling this method multiple times or after the other
	 * terminating methods has no effect (the value is {@link Operators#onNextDropped(Object, Context) dropped}).
	 * Calling this method with a {@code null} value will be silently accepted as a call to
	 * {@link #success()} by standard implementations.
	 *
	 * @param value the value to complete with
	 */
	void success(@Nullable T value);

	/**
	 * Terminate with the given exception
	 * <p>Calling this method multiple times or after the other terminating methods is
	 * an unsupported operation. It will discard the exception through the
	 * {@link Hooks#onErrorDropped(Consumer)} hook. This is to avoid
	 * complete and silent swallowing of the exception.
	 * @param e the exception to complete with
	 */
	void error(Throwable e);

	/**
	 * Return the current subscriber {@link Context}.
	 * <p>
	 *   {@link Context} can be enriched via {@link Mono#contextWrite(Function)}
	 *   operator or directly by a child subscriber overriding
	 *   {@link CoreSubscriber#currentContext()}
	 *
	 * @deprecated To be removed in 3.6.0 at the earliest. Prefer using #contextView() instead.
	 */
	@Deprecated
	Context currentContext();

	/**
	 * Return the current subscriber's context as a {@link ContextView} for inspection.
	 * <p>
	 * {@link Context} can be enriched downstream via {@link Mono#contextWrite(Function)}
	 * operator or directly by a child subscriber overriding {@link CoreSubscriber#currentContext()}.
	 *
	 * @return the current subscriber {@link ContextView}.
	 */
	default ContextView contextView() {
		return this.currentContext();
	}

	/**
	 * Attaches a {@link LongConsumer} to this {@link MonoSink} that will be notified of
	 * any request to this sink.
	 *
	 * @param consumer the consumer to invoke on request
	 *
	 * @return {@link MonoSink} with a consumer that is notified of requests
	 */
	MonoSink<T> onRequest(LongConsumer consumer);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link MonoSink} is
	 * cancelled. At most one callback can be registered, and subsequent calls to this method
	 * will result in the immediate disposal of the extraneous {@link Disposable}.
	 * <p>
	 * The callback is only relevant when the downstream {@link Subscription} is {@link Subscription#cancel() cancelled}.
	 *
	 * @param d the {@link Disposable} to use as a callback
	 * @return the {@link MonoSink} with a cancellation callback
	 * @see #onDispose(Disposable) onDispose(Disposable) for a callback that covers cancellation AND terminal signals
	 */
	MonoSink<T> onCancel(Disposable d);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link MonoSink} is effectively
	 * disposed, that is it cannot be used anymore. This includes both having played terminal
	 * signals (onComplete, onError) and having been cancelled (see {@link #onCancel(Disposable)}).
	 * At most one callback can be registered, and subsequent calls to this method will result in
	 * the immediate disposal of the extraneous {@link Disposable}.
	 * <p>
	 * Note that the "dispose" term is used from the perspective of the sink. Not to
	 * be confused with {@link Mono#subscribe()}'s {@link Disposable#dispose()} method, which
	 * maps to disposing the {@link Subscription} (effectively, a {@link Subscription#cancel()}
	 * signal).
	 *
	 * @param d the {@link Disposable} to use as a callback
	 * @return the {@link MonoSink} with a callback invoked on any terminal signal or on cancellation
	 * @see #onCancel(Disposable) onCancel(Disposable) for a cancellation-only callback
	 */
	MonoSink<T> onDispose(Disposable d);
}

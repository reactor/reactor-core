/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Wrapper API around an actual downstream Subscriber
 * for emitting nothing, a single value or an error (mutually exclusive).
 *
 * @param <T> the value type emitted
 */
public interface MonoSink<T> {

	/**
	 * Return the current subscriber {@link Context}.
	 * <p>
	 *   {@link Context} can be enriched via {@link Mono#subscriberContext(Function)}
	 *   operator or directly by a child subscriber overriding
	 *   {@link CoreSubscriber#currentContext()}
	 *
	 * @return the current subscriber {@link Context}.
	 */
	Context currentContext();

	/**
	 * Complete without any value. <p>Calling this method multiple times or after the
	 * other terminating methods has no effect.
	 */
	void success();

    /**
	 * Complete with the given value.
	 * <p>Calling this method multiple times or after the other
	 * terminating methods has no effect (the value is purely ignored). Calling this method with
	 * a {@code null} value will be silently accepted as a call to
	 * {@link #success()} by standard implementations.
	 *
	 * @param value the value to complete with
	 */
	void success(@Nullable T value);

	/**
	 * Terminate with the give exception
	 * <p>Calling this method multiple times or after the other terminating methods is
	 * an unsupported operation. It will discard the exception through the
	 * {@link Hooks#onErrorDropped(Consumer)} hook (which by default throws the exception
	 * wrapped via {@link reactor.core.Exceptions#bubble(Throwable)}). This is to avoid
	 * complete and silent swallowing of the exception.
	 *
	 * @param e the exception to complete with
	 */
	void error(Throwable e);

	/**
	 * Attaches a {@link LongConsumer} to this {@link MonoSink} that will be notified of
	 * any request to this sink.
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return {@link MonoSink} with a consumer that is notified of requests
	 */
	MonoSink<T> onRequest(LongConsumer consumer);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link MonoSink} is
	 * cancelled. This happens only when the downstream {@link Subscription}
	 * is {@link Subscription#cancel() cancelled}.
	 *
	 * @param d the {@link Disposable} to use as a callback
	 * @return the {@link MonoSink} with a cancellation callback
	 * @see #onCancel(Disposable) onDispose(Disposable) for a callback that covers cancellation AND terminal signals
	 */
	MonoSink<T> onCancel(Disposable d);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link MonoSink} is effectively
	 * disposed, that is it cannot be used anymore. This includes both having played terminal
	 * signals (onComplete, onError) and having been cancelled (see {@link #onCancel(Disposable)}).
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

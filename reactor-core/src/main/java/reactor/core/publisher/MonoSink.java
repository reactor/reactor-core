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

import reactor.core.Disposable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Wrapper API around an actual downstream Subscriber
 * for emitting nothing, a single value or an error (mutually exclusive).
 *
 * @param <T> the value type emitted
 */
public interface MonoSink<T> {

	/**
	 * Complete without any value.
	 * <p>Calling this method multiple times or after the other
	 * terminating methods has no effect.
	 */
	void success();

	/**
	 * Immediately propagate a {@link Context} to the child {@link Subscriber} given an
	 * eventually non empty parent {@link Context}.
	 *
	 * @param doOnContext a {@link Function} given the parent context and producing a
	 * new one to be pushed
	 * @return this sink
	 */
	MonoSink<T> contextualize(Function<Context, Context> doOnContext);

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
	 * Associates a disposable resource with this MonoSink that will be disposed on
	 * downstream.cancel().
	 *
	 * @param d the disposable callback to use
	 *
	 * @return the {@link MonoSink} with resource to be disposed on cancel signal
	 */
	MonoSink<T> onCancel(Disposable d);

	/**
	 * Associates a disposable resource with this MonoSink that will be disposed on the
	 * first terminate signal which may be a cancel, complete or error signal.
	 *
	 * @param d the disposable callback to use
	 *
	 * @return the {@link MonoSink} with resource to be disposed on first terminate signal
	 */
	MonoSink<T> onDispose(Disposable d);
}

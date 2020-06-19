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

import java.util.function.Consumer;

import org.reactivestreams.Subscriber;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A base interface for a "scalar" sink, a construct through which Reactive Streams
 * signals can be programmatically pushed with scalar (or in other words {@link Mono})
 * semantics.
 * Sinks can be used in the context of an operator that ties one sink per {@link Subscriber}
 * (thus allowing pushing data to a specific {@link Subscriber}, see {@link MonoSink})
 * or encourage fully standalone usage (see {@link reactor.core.publisher.Sinks.StandaloneMonoSink}).
 *
 * @author Simon Basl&eacute;
 */
public interface ScalarSink<T> {

	/**
	 * Complete without any value. <p>Calling this method multiple times or after the
	 * other terminating methods has no effect.
	 */
	void success();

	/**
	 * Complete with the given value.
	 * <p>Calling this method multiple times or after the other
	 * terminating methods has no effect (the value is {@link Operators#onNextDropped(Object, Context) dropped}).
	 * Calling this method with a {@code null} value will be silently accepted as a call to
	 * {@link #success()} by standard implementations.
	 *
	 * @param value the value to complete with
	 */
	void success(@Nullable T value);

	/**
	 * Terminate with the give exception
	 * <p>Calling this method multiple times or after the other terminating methods is
	 * an unsupported operation. It will discard the exception through the
	 * {@link Hooks#onErrorDropped(Consumer)} hook. This is to avoid
	 * complete and silent swallowing of the exception.
	 *
	 * @param e the exception to complete with
	 */
	void error(Throwable e);

}

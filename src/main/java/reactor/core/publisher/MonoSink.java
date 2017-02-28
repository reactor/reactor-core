/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import reactor.core.Cancellation;
import reactor.core.Disposable;

/**
 * Wrapper API around an actual downstream Subscriber
 * for emitting nothing, a single value or an error (mutually exclusive).
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
     * Complete with the given value.
     * <p>Calling this method multiple times or after the other
     * terminating methods has no effect. Calling this method with
     * a {@code null} value will be silently accepted as a call to
     * {@link #success()} by standard implementations.
     * @param value the value to complete with
     */
    void success(T value);

    /**
     * Terminate with the give exception
     * <p>Calling this method multiple times or after the other
     * terminating methods has no effect.
     * @param e the exception to complete with
     */
    void error(Throwable e);

	/**
	 * Associates a disposable resource with this MonoSink that will be disposed on
	 * downstream.cancel().
	 *
	 * @param d the disposable callback to use
	 * @return the {@link MonoSink} with resource to be disposed on cancel signal
	 */
	MonoSink<T> onCancel(Disposable d);

	/**
	 * Associates a disposable resource with this MonoSink that will be disposed on the
	 * first terminate signal which may be a cancel or complete signal.
	 *
	 * @param d the disposable callback to use
	 * @return the {@link MonoSink} with resource to be disposed on first terminate signal
	 */
	MonoSink<T> onTerminate(Disposable d);

    /**
     * Sets a cancellation callback triggered by
     * downstreams cancel().
     * <p>Calling this method more than once has no effect.
     * @param c the cancellation callback
     */
	@Deprecated
	void setCancellation(Cancellation c);
}

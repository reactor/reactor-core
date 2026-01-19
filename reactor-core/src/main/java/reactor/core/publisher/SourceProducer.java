/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;

/**
 *
 * {@link SourceProducer} is a {@link Publisher} that is {@link Scannable} for the
 * purpose of being tied back to a chain of operators. By itself it doesn't allow
 * walking the hierarchy of operators, as they can only be tied from downstream to upstream
 * by referencing their sources.
 *
 * @param <O> output operator produced type
 *
 * @author Simon Baslé
 */
interface SourceProducer<O> extends Scannable, Publisher<O> {

	@Override
	default @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return null;
		if (key == Attr.ACTUAL) return null;
		if (key == InternalProducerAttr.INSTANCE) return true;

		return null;
	}

	@Override
	default String stepName() {
		return "source(" + getClass().getSimpleName() + ")";
	}

	/**
	 * An internal hook for operators to instruct this {@link SourceProducer} to terminate
	 * and clean up its resources, bypassing the standard subscription-cancellation path.
	 * <p>
	 * This method is necessary for specific scenarios where a downstream consumer
	 * terminates prematurely *without* propagating a standard
	 * {@link org.reactivestreams.Subscription#cancel() cancel} signal upstream. The primary
	 * use case is for operators that short-circuit the stream, such as {@code take(0)}.
	 * <p>
	 * Without this direct termination signal, a hot source with a buffer (like a
	 * {@code Sinks.many().unicast().onBackpressureBuffer(queue)}) could be orphaned, leading to its buffered
	 * elements never being released and causing a memory leak.
	 * <p>
	 * This is not intended for public use and should only be called by Reactor framework
	 * operators. The default implementation is a no-op, allowing sources to opt in to this
	 * behavior only if they manage resources that require explicit cleanup in such scenarios.
	 *
	 * @see reactor.core.publisher.Flux#take(long)
	 * @see SinkManyUnicast
	 */
	default void terminateAndCleanup() {
		// Default implementation does nothing.
	}
}

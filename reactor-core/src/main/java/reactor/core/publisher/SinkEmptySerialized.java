/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Empty;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.stream.Stream;

class SinkEmptySerialized<T> extends SinksSpecs.AbstractSerializedSink
		implements InternalEmptySink<T>, ContextHolder {

	final Empty<T> sink;
	final ContextHolder contextHolder;

	SinkEmptySerialized(Empty<T> sink, ContextHolder contextHolder) {
		this.sink = sink;
		this.contextHolder = contextHolder;
	}

	@Override
	public final EmitResult tryEmitEmpty() {
		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitEmpty();
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public final EmitResult tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");

		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitError(t);
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public int currentSubscriberCount() {
		return sink.currentSubscriberCount();
	}

	@Override
	public Mono<T> asMono() {
		return sink.asMono();
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return sink.inners();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return sink.scanUnsafe(key);
	}

	@Override
	public Context currentContext() {
		return contextHolder.currentContext();
	}
}

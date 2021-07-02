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
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.stream.Stream;

final class SinkManySerialized<T> extends SinksSpecs.AbstractSerializedSink
		implements InternalManySink<T>, Scannable {

	final Sinks.Many<T> sink;
	final ContextHolder contextHolder;

	SinkManySerialized(Sinks.Many<T> sink, ContextHolder contextHolder) {
		this.sink = sink;
		this.contextHolder = contextHolder;
	}

	@Override
	public int currentSubscriberCount() {
		return sink.currentSubscriberCount();
	}

	@Override
	public Flux<T> asFlux() {
		return sink.asFlux();
	}

	@Override
	public Context currentContext() {
		return contextHolder.currentContext();
	}

	public boolean isCancelled() {
		return Scannable.from(sink).scanOrDefault(Attr.CANCELLED, false);
	}

	@Override
	public final Sinks.EmitResult tryEmitComplete() {
		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitComplete();
		} finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public final Sinks.EmitResult tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");

		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitError(t);
		} finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public final Sinks.EmitResult tryEmitNext(T t) {
		Objects.requireNonNull(t, "t is null in sink.next(t)");

		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitNext(t);
		} finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		return sink.scanUnsafe(key);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Scannable.from(sink).inners();
	}

	@Override
	public String toString() {
		return sink.toString();
	}
}

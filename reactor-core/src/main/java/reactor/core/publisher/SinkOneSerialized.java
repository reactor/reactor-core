/*
 * Copyright (c) 2020-Present VMware Inc. or its affiliates, All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *        https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.One;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class SinkOneSerialized<T> implements One<T>, ContextHolder {

	final One<T> sink;
	final ContextHolder contextHolder;

	volatile Throwable error;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkOneSerialized, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(SinkOneSerialized.class, Throwable.class, "error");

	volatile Thread lockedAt;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkOneSerialized, Thread> LOCKED_AT =
			AtomicReferenceFieldUpdater.newUpdater(SinkOneSerialized.class, Thread.class, "lockedAt");

	volatile AtomicBoolean done = new AtomicBoolean(false);

	public SinkOneSerialized(One<T> sink, ContextHolder contextHolder) {
		this.sink = sink;
		this.contextHolder = contextHolder;
	}

	@Override
	public Sinks.Emission tryEmitEmpty() {
		return tryEmitValue(null);
	}

	@Override
	public Sinks.Emission tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");
		if (done.get()) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		Thread lockedAt = this.lockedAt;
		if (!(lockedAt == null || lockedAt == Thread.currentThread())) {
			return Sinks.Emission.FAIL_NON_SERIALIZED;
		}
		if (!Exceptions.addThrowable(ERROR, this, t)) {
			return Sinks.Emission.FAIL_TERMINATED;
		}

		if (!done.compareAndSet(false, true)) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		return sink.tryEmitError(t);
	}

	@Override
	public Sinks.Emission tryEmitValue(T value) {
		if (done.get()) {
			return Sinks.Emission.FAIL_TERMINATED;
		}

		Thread currentThread = Thread.currentThread();
		Thread lockedAt = LOCKED_AT.get(this);
		if (lockedAt != null) {
			if (lockedAt != currentThread) {
				return Sinks.Emission.FAIL_NON_SERIALIZED;
			}
		}
		else if (!LOCKED_AT.compareAndSet(this, null, currentThread)) {
			return Sinks.Emission.FAIL_NON_SERIALIZED;
		}

		if (!done.compareAndSet(false, true)){
			LOCKED_AT.compareAndSet(this, currentThread, null);
			return Sinks.Emission.FAIL_TERMINATED;
		}

		Sinks.Emission emission = sink.tryEmitValue(value);
		LOCKED_AT.compareAndSet(this, currentThread, null);
		return emission;
	}

	@Override
	public void emitEmpty() {
		//no particular error condition handling for emitEmpty
		emitValue(null);
	}

	@Override
	public void emitError(Throwable error) {
		Sinks.Emission result = tryEmitError(error);
		switch (result) {
			case FAIL_TERMINATED:
			case FAIL_NON_SERIALIZED:
				Operators.onErrorDropped(error, currentContext());
				break;
		}
	}

	@Override
	public void emitValue(T value) {
		//no particular error condition handling for emitValue
		tryEmitValue(value);
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
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ERROR) {
			return error;
		}
		if (key == Attr.TERMINATED) {
			return done;
		}

		return sink.scanUnsafe(key);
	}

	@Override
	public Context currentContext() {
		return contextHolder.currentContext();
	}
}

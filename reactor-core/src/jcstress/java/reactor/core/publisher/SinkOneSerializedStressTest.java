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

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.LLI_Result;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinkOneSerializedStressTest {

	final SinkOneSerializedStressTest.TargetSink<Object> stressSink = new SinkOneSerializedStressTest.TargetSink<>();

	final SinkOneSerialized<Object> sink = new SinkOneSerialized<>(
			stressSink,
			Context::empty
	);

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED, 1"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK, 1"}, expect = ACCEPTABLE, desc = "second wins")
	@Outcome(id = {"OK, FAIL_TERMINATED, 1"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_TERMINATED, OK, 1"}, expect = ACCEPTABLE, desc = "second wins")
	@State
	public static class TryEmitValueStressTest extends SinkOneSerializedStressTest {

		@Actor
		public void first(LLI_Result r) {
			r.r1 = sink.tryEmitValue("foo");
		}

		@Actor
		public void second(LLI_Result r) {
			r.r2 = sink.tryEmitValue("bar");
		}

		@Arbiter
		public void arbiter(LLI_Result r) {
			r.r3 = stressSink.onValueCall.get();
		}
	}

	static class TargetSink<T> implements Sinks.One<T> {

		final AtomicReference<StressSubscriber.Operation> guard = new AtomicReference<>(null);

		final AtomicInteger onValueCall = new AtomicInteger();

		@Override
		public Sinks.Emission tryEmitEmpty() {
			return tryEmitValue(null);
		}

		@Override
		public Sinks.Emission tryEmitError(Throwable error) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_ERROR)) {
				throw new IllegalStateException("SinkOneSerialized should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			guard.compareAndSet(StressSubscriber.Operation.ON_ERROR, null);
			return Sinks.Emission.OK;
		}

		@Override
		public Sinks.Emission tryEmitValue(T value) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_COMPLETE)) {
				throw new IllegalStateException("SinkOneSerialized should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			onValueCall.incrementAndGet();
			guard.compareAndSet(StressSubscriber.Operation.ON_COMPLETE, null);
			return Sinks.Emission.OK;
		}

		@Override
		public void emitEmpty() {
			tryEmitValue(null).orThrow();
		}

		@Override
		public void emitError(Throwable error) {
			tryEmitError(error).orThrowWithCause(error);
		}

		@Override
		public void emitValue(T value) {
			tryEmitValue(value).orThrow();
		}

		@Override
		public int currentSubscriberCount() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Mono<T> asMono() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

	}
}

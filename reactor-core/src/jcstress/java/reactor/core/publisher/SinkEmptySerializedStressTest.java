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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.LLI_Result;
import reactor.util.context.Context;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinkEmptySerializedStressTest {

	final TargetSink<Object> stressSink = new TargetSink<>();

	final SinkEmptySerialized<Object> sink = new SinkEmptySerialized<>(
			stressSink,
			Context::empty
	);

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED, 1"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK, 1"}, expect = ACCEPTABLE, desc = "second wins")
	@Outcome(id = {"OK, FAIL_TERMINATED, 2"}, expect = ACCEPTABLE, desc = "second after first")
	@Outcome(id = {"FAIL_TERMINATED, OK, 2"}, expect = ACCEPTABLE, desc = "first after second")
	@State
	public static class TryEmitEmptyStressTest extends SinkEmptySerializedStressTest {

		@Actor
		public void first(LLI_Result r) {
			r.r1 = sink.tryEmitEmpty();
		}

		@Actor
		public void second(LLI_Result r) {
			r.r2 = sink.tryEmitEmpty();
		}

		@Arbiter
		public void arbiter(LLI_Result r) {
			r.r3 = stressSink.onEmptyCall.get();
		}
	}

	static class TargetSink<T> implements InternalEmptySink<T> {

		final AtomicReference<StressSubscriber.Operation> guard = new AtomicReference<>(null);

		final AtomicInteger onEmptyCall = new AtomicInteger();

		final AtomicBoolean done = new AtomicBoolean(false);

		@Override
		public Sinks.EmitResult tryEmitEmpty() {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_COMPLETE)) {
				throw new IllegalStateException("SinkEmptySerialized should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			onEmptyCall.incrementAndGet();
			guard.compareAndSet(StressSubscriber.Operation.ON_COMPLETE, null);
			return done.compareAndSet(false, true) ? Sinks.EmitResult.OK : Sinks.EmitResult.FAIL_TERMINATED;
		}

		@Override
		public Sinks.EmitResult tryEmitError(Throwable error) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_ERROR)) {
				throw new IllegalStateException("SinkEmptySerialized should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			guard.compareAndSet(StressSubscriber.Operation.ON_ERROR, null);
			return done.compareAndSet(false, true) ? Sinks.EmitResult.OK : Sinks.EmitResult.FAIL_TERMINATED;
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

		@Override
		public Context currentContext() {
			return Context.empty();
		}
	}

}

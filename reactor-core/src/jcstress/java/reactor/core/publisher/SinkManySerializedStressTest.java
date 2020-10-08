/*
 * Copyright (c) 2020-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLI_Result;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinkManySerializedStressTest {

	final TargetSink<Object> stressSink = new TargetSink<>();

	final SinkManySerialized<Object> sink = new SinkManySerialized<>(
			stressSink,
			Context::empty
	);

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED, 1"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK, 1"}, expect = ACCEPTABLE, desc = "second wins")
	@Outcome(id = {"OK, OK, 2"}, expect = ACCEPTABLE, desc = "one after another")
	@State
	public static class TryEmitNextStressTest extends SinkManySerializedStressTest {

		@Actor
		public void first(LLI_Result r) {
			r.r1 = sink.tryEmitNext("Hello");
		}

		@Actor
		public void second(LLI_Result r) {
			r.r2 = sink.tryEmitNext("Hello");
		}

		@Arbiter
		public void arbiter(LLI_Result r) {
			r.r3 = stressSink.onNextCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED, 0"}, expect = ACCEPTABLE, desc = "onNext wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK, 1"}, expect = ACCEPTABLE, desc = "onComplete wins")
	@Outcome(id = {"FAIL_TERMINATED, OK, 1"}, expect = ACCEPTABLE, desc = "onNext after onComplete")
	@Outcome(id = {"OK, OK, 1"}, expect = ACCEPTABLE, desc = "onComplete after onNext")
	@State
	public static class TerminatedVsOnNextStressTest extends SinkManySerializedStressTest {

		@Actor
		public void first(LLI_Result r) {
			r.r1 = sink.tryEmitNext("Hello");
		}

		@Actor
		public void second(LLI_Result r) {
			r.r2 = sink.tryEmitComplete();
		}

		@Arbiter
		public void arbiter(LLI_Result r) {
			r.r3 = stressSink.onCompleteCalls.get();
		}
	}

	static class TargetSink<T> implements InternalManySink<T> {

		final AtomicReference<StressSubscriber.Operation> guard = new AtomicReference<>(null);

		final AtomicInteger onNextCalls = new AtomicInteger();

		final AtomicInteger onCompleteCalls = new AtomicInteger();

		@Override
		public Context currentContext() {
			return Context.empty();
		}

		@Override
		public Sinks.EmitResult tryEmitNext(T t) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_NEXT)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			onNextCalls.incrementAndGet();
			guard.compareAndSet(StressSubscriber.Operation.ON_NEXT, null);
			return Sinks.EmitResult.OK;
		}

		@Override
		public Sinks.EmitResult tryEmitComplete() {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_COMPLETE)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			onCompleteCalls.incrementAndGet();
			guard.compareAndSet(StressSubscriber.Operation.ON_COMPLETE, null);
			return Sinks.EmitResult.OK;
		}

		@Override
		public Sinks.EmitResult tryEmitError(Throwable error) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_ERROR)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			guard.compareAndSet(StressSubscriber.Operation.ON_ERROR, null);
			return Sinks.EmitResult.OK;
		}

		@Override
		public int currentSubscriberCount() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Flux<T> asFlux() {
			throw new UnsupportedOperationException();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}
	}
}

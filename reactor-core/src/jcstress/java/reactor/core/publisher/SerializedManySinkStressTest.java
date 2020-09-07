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
import reactor.util.context.Context;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SerializedManySinkStressTest {

	final TargetSink<Object> stressSink = new TargetSink<>();

	final SerializedManySink<Object> sink = new SerializedManySink<>(
			stressSink,
			Context::empty
	);

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED, 1"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK, 1"}, expect = ACCEPTABLE, desc = "second wins")
	@Outcome(id = {"OK, OK, 2"}, expect = ACCEPTABLE, desc = "one after another")
	@State
	public static class TryEmitNextStressTest extends SerializedManySinkStressTest {

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

	static class TargetSink<T> implements Sinks.Many<T> {

		final AtomicReference<StressSubscriber.Operation> guard = new AtomicReference<>(null);

		final AtomicInteger onNextCalls = new AtomicInteger();

		@Override
		public Sinks.Emission tryEmitNext(T t) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_NEXT)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			onNextCalls.incrementAndGet();
			guard.compareAndSet(StressSubscriber.Operation.ON_NEXT, null);
			return Sinks.Emission.OK;
		}

		@Override
		public Sinks.Emission tryEmitComplete() {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_COMPLETE)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			guard.compareAndSet(StressSubscriber.Operation.ON_COMPLETE, null);
			return Sinks.Emission.OK;
		}

		@Override
		public Sinks.Emission tryEmitError(Throwable error) {
			if (!guard.compareAndSet(null, StressSubscriber.Operation.ON_ERROR)) {
				throw new IllegalStateException("SerializedManySink should protect from non-serialized access");
			}

			LockSupport.parkNanos(10);
			guard.compareAndSet(StressSubscriber.Operation.ON_ERROR, null);
			return Sinks.Emission.OK;
		}

		@Override
		public void emitNext(T t) {
			tryEmitNext(t).orThrow();
		}

		@Override
		public void emitError(Throwable error) {
			tryEmitError(error).orThrow();
		}

		@Override
		public void emitComplete() {
			tryEmitComplete().orThrow();
		}

		@Override
		public Flux<T> asFlux() {
			throw new UnsupportedOperationException();
		}
	}
}

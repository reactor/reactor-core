package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LL_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SerializedManySinkStressTest {

	final SerializedManySink<Object> sink = new SerializedManySink<>(
			Sinks.many().unsafe().multicast().onBackpressureBuffer(),
			Operators.emptySubscriber()
	);

	@JCStressTest
	@Outcome(id = {"OK, FAIL_NON_SERIALIZED"}, expect = ACCEPTABLE, desc = "first wins")
	@Outcome(id = {"FAIL_NON_SERIALIZED, OK"}, expect = ACCEPTABLE, desc = "second wins")
	@Outcome(id = {"OK, OK"}, expect = ACCEPTABLE, desc = "one after another")
	@State
	public static class TryEmitNextStressTest extends SerializedManySinkStressTest {

		@Actor
		public void first(LL_Result r) {
			r.r1 = sink.tryEmitNext("Hello");
		}

		@Actor
		public void second(LL_Result r) {
			r.r2 = sink.tryEmitNext("Hello");
		}
	}

}

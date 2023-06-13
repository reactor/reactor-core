package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIII_Result;
import org.openjdk.jcstress.infra.results.III_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxPublishStressTest {

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class ConcurrentSubscriptionStressTest {

		final Flux<Integer> sharedSource = Flux.range(0, 10).hide().publish().refCount(2);

		final StressSubscriber<Integer> subscriber1 = new StressSubscriber<>();
		final StressSubscriber<Integer> subscriber2 = new StressSubscriber<>();

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}
		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

		@Actor
		@Arbiter
		public void arbiter(IIIIII_Result r) {
			r.r1 = subscriber1.onNextCalls.get();
			r.r2 = subscriber2.onNextCalls.get();
			r.r3 = subscriber1.onCompleteCalls.get();
			r.r4 = subscriber2.onCompleteCalls.get();
			r.r5 = subscriber1.onErrorCalls.get();
			r.r6 = subscriber2.onErrorCalls.get();
		}
	}
}

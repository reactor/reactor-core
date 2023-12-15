package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLL_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class MonoReduceStressTest {

	@JCStressTest
	@Outcome(id = {
			"0, true, ((), 1, 2)",
			"0, true, (1, (), 2)",
			"0, true, (1, 2, ())",
			"0, true, ((1), 2)",
			"0, true, (2, (1))",
			"0, true, ((1, 2))"
	}, expect = ACCEPTABLE, desc = "")
	@State
	public static class ReduceSeedOnNextAndCancelStressTest {

		final StressSubscriber<Object> sr = new StressSubscriber<>();
		final List<Integer> list = new ArrayList<>();

		final MonoReduceSeed.ReduceSeedSubscriber<Integer, List<Integer>> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(sr,
						(l, next) -> {
							l.add(next);
							return l;
						},
						list);
		final StressSubscription<Integer> s = new StressSubscription<>(sub);

		{
			sub.onSubscribe(s);
		}


		@Actor
		public void emit() {
			sub.onNext(1);
			sub.onNext(2);
		}

		@Actor
		public void cancel() {
			sub.cancel();
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = sr.onNextCalls.get() + sr.onCompleteCalls.get() + sr.onErrorCalls.get();
			result.r2 = s.cancelled.get();
			result.r3 = sr.discardedValues.toString()
			                              .replace("[", "(")
			                              .replace("]", ")");
		}


	}
}

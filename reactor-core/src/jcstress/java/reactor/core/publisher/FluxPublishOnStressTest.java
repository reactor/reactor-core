package reactor.core.publisher;

import java.util.concurrent.ForkJoinPool;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class FluxPublishOnStressTest {

	@JCStressTest
	@Outcome(id = {"0, 1", "0, 0"}, expect = ACCEPTABLE, desc = "no errors propagated after cancellation because of disposed worker")
	@State
	public static class FluxPublishOnOnNextAndCancelRaceStressTest {
		final StressSubscription<Integer> upstream = new StressSubscription<>(null);
		final StressSubscriber<Integer> downstream = new StressSubscriber<>();
		final Scheduler scheduler =
				Schedulers.fromExecutorService(ForkJoinPool.commonPool());

		final FluxPublishOn.PublishOnSubscriber<Integer> publishOnSubscriber =
				new FluxPublishOn.PublishOnSubscriber<>(downstream,
						scheduler,
						scheduler.createWorker(), true, 32, 12, Queues.get(32));


		{
			publishOnSubscriber.onSubscribe(upstream);
		}

		@Actor
		public void produce() {
			publishOnSubscriber.onNext(1);
			publishOnSubscriber.onNext(2);
			publishOnSubscriber.onNext(3);
			publishOnSubscriber.onNext(4);
		}

		@Actor
		public void cancel() {
			publishOnSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result result) {
			result.r1 = downstream.onErrorCalls.get();
			result.r2 = downstream.droppedErrors.size();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, 1", "0, 0"}, expect = ACCEPTABLE, desc = "no errors propagated after cancellation because of disposed worker")
	@State
	public static class FluxPublishOnConditionalOnNextAndCancelRaceStressTest {
		final StressSubscription<Integer> upstream = new StressSubscription<>(null);
		final ConditionalStressSubscriber<Integer> downstream = new ConditionalStressSubscriber<>();
		final Scheduler scheduler =
				Schedulers.fromExecutorService(ForkJoinPool.commonPool());

		final FluxPublishOn.PublishOnConditionalSubscriber<Integer> publishOnSubscriber =
				new FluxPublishOn.PublishOnConditionalSubscriber<>(downstream,
						scheduler,
						scheduler.createWorker(), true, 32, 12, Queues.get(32));


		{
			publishOnSubscriber.onSubscribe(upstream);
		}

		@Actor
		public void produce() {
			publishOnSubscriber.onNext(1);
			publishOnSubscriber.onNext(2);
			publishOnSubscriber.onNext(3);
			publishOnSubscriber.onNext(4);
		}

		@Actor
		public void cancel() {
			publishOnSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result result) {
			result.r1 = downstream.onErrorCalls.get();
			result.r2 = downstream.droppedErrors.size();
		}
	}
}

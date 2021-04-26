package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;
import org.openjdk.jcstress.infra.results.IIZ_Result;
import org.openjdk.jcstress.infra.results.II_Result;

import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

public abstract class MonoDelayStressTest {

	@JCStressTest
	@Outcome(id = {"1, 0, true"}, expect = ACCEPTABLE, desc = "Request before tick was delivered")
	@Outcome(id = {"1, 0, false"}, expect = ACCEPTABLE, desc = "Request AFTER tick was delivered")
	@State
	public static class RequestAndRunStressTest {

		/*
		Implementation notes: in this test we use the VirtualTimeScheduler to better coordinate
		the triggering of the `run` method. We also use the hasProgressed AtomicBoolean to
		track whenever the delayTrigger happens before the subscribe actor.
		 */

		final StressSubscriber<Long> subscriber = new StressSubscriber<>(1L);
		final VirtualTimeScheduler   virtualTimeScheduler;
		final MonoDelay              monoDelay;
		final AtomicBoolean          hasProgressed = new AtomicBoolean();

		{
			virtualTimeScheduler = VirtualTimeScheduler.create();
			monoDelay = new MonoDelay(1, TimeUnit.NANOSECONDS, virtualTimeScheduler);
			monoDelay.subscribe(subscriber);
		}

		@Actor
		public void delayTrigger(IIZ_Result r) {
			if (hasProgressed.compareAndSet(false, true)) {
				r.r3 = true; //requested before trigger
			}
			virtualTimeScheduler.advanceTimeBy(Duration.ofNanos(1));
		}

		@Actor
		public void request(IIZ_Result r) {
			if (hasProgressed.compareAndSet(false, true)) {
				r.r3 = false; //requested after trigger
			}
			subscriber.request(1);
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onErrorCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 0, 1"}, expect = ACCEPTABLE, desc = "Tick was delivered, request happened before tick")
	@Outcome(id = {"1, 0, 2"}, expect = ACCEPTABLE, desc = "Tick was delivered, request happened after tick")
	@Outcome(id = {"0, 0, 1"}, expect = ACCEPTABLE, desc = "Tick was cancelled, request happened before tick")
	@Outcome(id = {"0, 0, 2"}, expect = ACCEPTABLE, desc = "Tick was cancelled, request happened after tick")
	@Outcome(id = {"0, 0, 3"}, expect = ACCEPTABLE, desc = "Tick was cancelled before any interaction")
	@Outcome(id = {"1, 0, 3"}, expect = ACCEPTABLE_INTERESTING, desc = "Tick was cancelled before any interaction, but the delivery still happened")
	@State
	public static class RequestAndCancelStressTest {

		/*
		Implementation notes: in this test we use the VirtualTimeScheduler to better coordinate
		the triggering of the `run` method. We also use the firstStep AtomicInteger to
		track which actor gets to run first (1 is request, 2 is delay tick, 3 is cancel)
		 */

		final StressSubscriber<Long> subscriber = new StressSubscriber<>(1L);
		final VirtualTimeScheduler   virtualTimeScheduler;
		final MonoDelay              monoDelay;
		final AtomicInteger          firstStep = new AtomicInteger();

		{
			virtualTimeScheduler = VirtualTimeScheduler.create();
			monoDelay = new MonoDelay(1, TimeUnit.NANOSECONDS, virtualTimeScheduler);
			monoDelay.subscribe(subscriber);
		}

		@Actor
		public void request() {
			firstStep.compareAndSet(0, 1);
			subscriber.request(1);
		}

		@Actor
		public void delayTrigger() {
			firstStep.compareAndSet(0, 2);
			virtualTimeScheduler.advanceTimeBy(Duration.ofNanos(1));
		}

		@Actor
		public void cancelFromActual() {
			firstStep.compareAndSet(0, 3);
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onErrorCalls.get();
			r.r3 = firstStep.get();
		}
	}
}

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLL_Result;
import org.openjdk.jcstress.infra.results.LL_Result;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static reactor.core.publisher.FluxWindowTimeout.InnerWindow.formatState;

public class FluxWindowTimeoutStressTest {

//	public static void main(String[] args) {
//		for (;;) {
//			System.out.println("ping");
//			final FluxWindowTimoutStressTest1 test1 = new FluxWindowTimoutStressTest1();
//			RaceTestUtils.race(200, Schedulers.parallel(), () -> test1.request(),
//					() -> test1.next());
//		}
//	}

	@JCStressTest
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest1 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1;
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(1) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						if (index++ == 0) {
							subscriber1 = new InnerStressSubscriber<>(this);
							window.subscribe(subscriber1);
						}
						else {
							window.subscribe(subscriber2);
						}
					}
				};
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler);
		final StressSubscription<Long> subscription            =
				new StressSubscription<>(windowTimeoutSubscriber);

		{
			windowTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			windowTimeoutSubscriber.onNext(0L);
			windowTimeoutSubscriber.onNext(1L);
			windowTimeoutSubscriber.onComplete();
		}

		@Actor
		public void request() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 = subscriber1.onNextCalls.get() + subscriber2.onNextCalls.get();
			result.r2 =
					subscriber1.onCompleteCalls.get() + subscriber2.onCompleteCalls.get();
			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = subscription.requested;



			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result, mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() != 1) {
//				throw new IllegalStateException("s : " + formatState(windowTimeoutSubscriber.window.state) + "\n" +
//						windowTimeoutSubscriber.producerIndex + " " + windowTimeoutSubscriber.window.received
//								+ "s1 : " + formatState(((FluxWindowTimeout.InnerWindow) subscriber1.subscription).state) + " q: " + ((FluxWindowTimeout.InnerWindow) subscriber1.subscription).received +
//							(subscriber2.subscription != null ? ("\n" +
//					"s2 : " + formatState(((FluxWindowTimeout.InnerWindow) subscriber2.subscription).state) + " q: " + ((FluxWindowTimeout.InnerWindow) subscriber2.subscription).received) : "")
//				);
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString());
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"5, 1"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutInnerWindowStressTest {

		final VirtualTimeScheduler                                            virtualTimeScheduler =
				VirtualTimeScheduler.create();
		final StressSubscriber<Flux<Long>>                                    downstream           =
				new StressSubscriber<>(0);
		final StressSubscriber<Long>                                          subscriber           =
				new StressSubscriber<>(0);
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> parent               =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(downstream,
						10,
						10,
						TimeUnit.SECONDS,
						virtualTimeScheduler);
		final StressSubscription<Long>                                        upstream             =
				new StressSubscription<>(parent);
		final FluxWindowTimeout.InnerWindow<Long>                             inner                =
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1);

		{
			parent.onSubscribe(upstream);
		}

		@Actor
		public void sendNext() {
			inner.sendNext(1L);
			inner.sendNext(2L);
			inner.sendNext(3L);
			inner.sendNext(4L);
			inner.sendNext(5L);
			inner.sendCompleteByParent();
		}

		@Actor
		public void sendSubscribeAndRequest() {
			inner.subscribe(subscriber);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
		}

		@Arbiter
		public void arbiter(LL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get() + subscriber.onErrorCalls.get() * 2L;

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext");
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete");
			}

		}

	}

}

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLL_Result;
import org.openjdk.jcstress.infra.results.LLL_Result;
import org.openjdk.jcstress.infra.results.LL_Result;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;
import reactor.util.concurrent.Queues;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

public class FluxWindowTimeoutStressTest {

	@JCStressTest
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest1_0 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1;
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(1) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						int i = index;
						index++;
						if (i == 0) {
							subscriber1 = new InnerStressSubscriber<>(this);
							window.subscribe(subscriber1);
						}
						else if (i == 1) {
							window.subscribe(subscriber2);
						} else {
							throw new RuntimeException("unexpected window delivered");
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

			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void request() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + " " + windowTimeoutSubscriber.signals);
			}

			if (result.toString().startsWith("1") || result.toString().startsWith("0") || result.toString().startsWith("2, 1, 2")) {
				throw new IllegalStateException("boom " + result + " " + windowTimeoutSubscriber.signals.toString());
			}
		}
	}


	@JCStressTest
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest1_1 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(2) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						if (index++ == 0) {
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
			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void request() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + " " + windowTimeoutSubscriber.signals);
			}

			if (result.toString().startsWith("2, 1, 2")) {
				throw new IllegalStateException("boom " + result + " " + windowTimeoutSubscriber.signals.toString());
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 3, 3, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 3, 3, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest1_2 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber3 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber4 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(3) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						switch (index++) {
							case 0:
								window.subscribe(subscriber1);
								break;
							case 1:
								window.subscribe(subscriber2);
								break;
							case 2:
								window.subscribe(subscriber3);
								break;
							case 3:
								window.subscribe(subscriber4);
								break;
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
			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void advanceTime() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void requestMain() {
			try {
				mainSubscriber.request(1);
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 =
					subscriber1.onNextCalls.get() + subscriber2.onNextCalls.get() + subscriber3.onNextCalls.get() +  subscriber4.onNextCalls.get();
			result.r2 =
					subscriber1.onCompleteCalls.get() + subscriber2.onCompleteCalls.get() + subscriber3.onCompleteCalls.get() +  subscriber4.onCompleteCalls.get();
			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = subscription.requested;



			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() != 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + " " + windowTimeoutSubscriber.signals);
			}

			if (result.toString().equals("1, 2, 2, 1, 2")) {
				throw new IllegalStateException("boom" + windowTimeoutSubscriber.signals.toString());
			}
		}
	}

	public static void main(String[] args) {

		for (int i = 0; i < 1000000; i++) {
			FluxWindowTimoutStressTest1_3 test1_3 = new FluxWindowTimoutStressTest1_3();
			if (i % 1000 == 0) {
				System.out.println("---" + i + "---");
			}
			try {
				RaceTestUtils.race(20, Schedulers.boundedElastic(), test1_3::next, test1_3::requestMain, test1_3::advanceTime);
			} catch (Throwable t) {
				throw new RuntimeException(test1_3.windowTimeoutSubscriber.signals.toString(), t);
			}

			test1_3.arbiter(new LLLLL_Result());
		}
	}

	@JCStressTest
	@Outcome(id = {
			"8, 4, 4, 1, 8",
			"8, 5, 5, 1, 8",
			"8, 5, 5, 1, 9",
			"8, 5, 5, 1, 10",
			"8, 6, 6, 1, 8",
			"8, 6, 6, 1, 9",
			"8, 6, 6, 1, 10",
			"8, 7, 7, 1, 8",
			"8, 7, 7, 1, 9",
			"8, 7, 7, 1, 10",
			"8, 8, 8, 1, 8",
			"8, 8, 8, 1, 9",
			"8, 8, 8, 1, 10",
			"8, 9, 9, 1, 8",
			"8, 9, 9, 1, 9",
			"8, 9, 9, 1, 10",
			"8, 9, 9, 0, 8",
			"8, 9, 9, 0, 9",
			"8, 9, 9, 0, 10",
	}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest1_3 {



		final UnicastProcessor<Long> proxy = new UnicastProcessor<>(Queues.<Long>get(8).get());
		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber3 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber4 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber5 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber6 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber7 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber8 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber9 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(1) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						switch (index++) {
							case 0:
								subscriber1 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber1);
								break;
							case 1:
								subscriber2 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber2);
								break;
							case 2:
								subscriber3 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber3);
								break;
							case 3:
								subscriber4 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber4);
								break;
							case 4:
								subscriber5 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber5);
								break;
							case 5:
								subscriber6 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber6);
								break;
							case 6:
								subscriber7 = new InnerStressSubscriber<>(this);
								window.subscribe(subscriber7);
								break;
							case 7:
								window.subscribe(subscriber8);
								break;
							case 8:
								window.subscribe(subscriber9);
								break;
						}
					}
				};
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler);
		final AtomicLong requested = new AtomicLong();

		{
			proxy.doOnRequest(requested::addAndGet)
			     .subscribe(windowTimeoutSubscriber);
		}

		@Actor
		public void next() {
			proxy.onNext(0L);
			proxy.onNext(1L);
			proxy.onNext(2L);
			proxy.onNext(3L);
			proxy.onNext(4L);
			proxy.onNext(5L);
			proxy.onNext(6L);
			proxy.onNext(7L);
			try {
				proxy.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void advanceTime() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Actor
		public void requestMain() {
			try {
				mainSubscriber.request(1);
			}
			catch (Exception e) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString(), e);
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 =
					(subscriber1.onNextCalls.get()) +
							(subscriber2.onNextCalls.get()) +
							(subscriber3.onNextCalls.get()) +
							(subscriber4.onNextCalls.get()) +
							(subscriber5.onNextCalls.get()) +
							(subscriber6.onNextCalls.get()) +
							(subscriber7.onNextCalls.get()) +
							(subscriber8.onNextCalls.get()) +
							(subscriber9.onNextCalls.get());
			result.r2 =
					(subscriber1.onCompleteCalls.get()) +
							(subscriber2.onCompleteCalls.get()) +
							(subscriber3.onCompleteCalls.get()) +
							(subscriber4.onCompleteCalls.get()) +
							(subscriber5.onCompleteCalls.get()) +
							(subscriber6.onCompleteCalls.get()) +
							(subscriber7.onCompleteCalls.get()) +
							(subscriber8.onCompleteCalls.get()) +
							(subscriber9.onCompleteCalls.get());

			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = requested.get();



			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + " " + windowTimeoutSubscriber.signals);
			}

			if (result.toString().startsWith("6") || result.toString().startsWith("7") || result.toString().startsWith("8, 4, 5") || result.toString().startsWith("8, 5, 6")) {
				throw new RuntimeException("boom " + result + " "+ mainSubscriber.receivedValues + " " + windowTimeoutSubscriber.signals);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"2, 0, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 3, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 3, 3, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 3, 3, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 3, 3, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest2_0 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber3 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber          =
				new StressSubscriber<Flux<Long>>(3) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						switch (index++) {
							case 0:
								window.subscribe(subscriber1);
								break;
							case 1:
								window.subscribe(subscriber2);
								break;
							case 2:
								window.subscribe(subscriber3);
								break;
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
			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException("next" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void advanceTime() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException("advanceTime" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void cancel() {
			try {
				subscriber1.cancel();
			}
			catch (Exception e) {
				throw new IllegalStateException("cancel" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 =
					subscriber1.onNextCalls.get() + subscriber1.onNextDiscarded.get() + subscriber2.onNextCalls.get() + subscriber3.onNextCalls.get();
			result.r2 =
					subscriber1.onCompleteCalls.get() + subscriber2.onCompleteCalls.get()  + subscriber3.onCompleteCalls.get();
			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = subscription.requested;

			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString());
			}

			if (result.toString().startsWith("2, 2, 3, 0, 3") || result.toString().startsWith("2, 2, 3, 0, 4")) {
				throw new IllegalStateException("boom" + windowTimeoutSubscriber.signals.toString());
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"2, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 2, 2, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 1, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest2_1 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1;
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber =
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
			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException("next" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void advanceTime() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException("advanceTime" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void cancel() {
			try {
				subscriber2.cancel();
			}
			catch (Exception e) {
				throw new IllegalStateException("cancel" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 =
					subscriber1.onNextCalls.get() + subscriber1.onNextDiscarded.get()
							+ subscriber2.onNextCalls.get() + subscriber2.onNextDiscarded.get();
			result.r2 =
					subscriber1.onCompleteCalls.get() + subscriber2.onCompleteCalls.get();
			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = subscription.requested;

			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException(windowTimeoutSubscriber.signals.toString());
			}

			if (result.toString().startsWith("2, 1, 3") || result.toString().startsWith("3, 2, 2")) {
				throw new IllegalStateException("boom" + windowTimeoutSubscriber.signals.toString());
			}
		}
	}



	@JCStressTest
	@Outcome(id = {"4, 1, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 1, 1, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 1, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 1, 1, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 2, 2, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 0, 5"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 1, 5"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 1, 6"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 3, 3, 0, 6"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 0, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 0, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 0, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 1, 4"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 0, 5"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 1, 5"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 0, 6"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"4, 4, 4, 1, 6"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutStressTest2_2 {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();
		StressSubscriber<Long> subscriber1 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber2 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber3 = new StressSubscriber<>();
		StressSubscriber<Long> subscriber4 = new StressSubscriber<>();
		final StressSubscriber<Flux<Long>> mainSubscriber =
				new StressSubscriber<Flux<Long>>(3) {
					int index = 0;

					@Override
					public void onNext(Flux<Long> window) {
						super.onNext(window);
						switch (index++) {
							case 0:
								window.subscribe(subscriber1);
								break;
							case 1:
								window.subscribe(subscriber2);
								break;
							case 2:
								window.subscribe(subscriber3);
								break;
							case 3:
								window.subscribe(subscriber4);
								break;
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
			windowTimeoutSubscriber.onNext(2L);
			windowTimeoutSubscriber.onNext(3L);
			try {
				windowTimeoutSubscriber.onComplete();
			}
			catch (Exception e) {
				throw new IllegalStateException("next" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void advanceTime() {
			try {
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
				virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			}
			catch (Exception e) {
				throw new IllegalStateException("advanceTime" + windowTimeoutSubscriber.signals, e);
			}
		}

		@Actor
		public void cancel() {
			mainSubscriber.cancel();
		}

		@Actor
		public void request() {
			mainSubscriber.request(1);
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			long extraDiscarded = 0;
			for (Object discarded : mainSubscriber.discardedValues) {
				if (discarded instanceof Flux) {
					final StressSubscriber<Long> subscriber = new StressSubscriber<>(0);
					((Flux<Long>) discarded).subscribe(subscriber);
					subscriber.cancel();
					extraDiscarded += subscriber.onNextDiscarded.get();
				} else {
					extraDiscarded++;
				}
			}
			result.r1 =
					subscriber1.onNextCalls.get() + subscriber1.onNextDiscarded.get()
							+ subscriber2.onNextCalls.get() + subscriber2.onNextDiscarded.get()
							+ subscriber3.onNextCalls.get() + subscriber3.onNextDiscarded.get()
							+ subscriber4.onNextCalls.get() + subscriber4.onNextDiscarded.get()
							+ extraDiscarded;
			result.r2 =
					subscriber1.onCompleteCalls.get() + subscriber2.onCompleteCalls.get() + subscriber3.onCompleteCalls.get() + subscriber4.onCompleteCalls.get();
			result.r3 = mainSubscriber.onNextCalls.get();
			result.r4 = mainSubscriber.onCompleteCalls.get();
			result.r5 = subscription.requested;

			if (mainSubscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result  + windowTimeoutSubscriber.signals.toString(), mainSubscriber.stacktraceOnComplete);
			}

			if (result.toString().startsWith("3, ")) {
				throw new IllegalStateException("boom " + result + " " + subscriber1.receivedValues + " " +  subscriber1.discardedValues + " "
						+ subscriber2.receivedValues + " " +  subscriber2.discardedValues + " "
						+ subscriber3.receivedValues + " " +  subscriber3.discardedValues + " "
						+ subscriber4.receivedValues + " " +  subscriber4.discardedValues + " "
						+  windowTimeoutSubscriber.signals);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
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
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false);

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
			inner.sendComplete();
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

			if (result.toString().equals("0, 1")) {
				throw new IllegalStateException("boom " + parent.signals.toString());
			}

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext");
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete");
			}

		}

	}

	@JCStressTest
	@Outcome(id = {"5, 1, true"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"5, 1, false"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"5, 0, true"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutInnerWindowStressTest1 {

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
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false);

		{
			parent.onSubscribe(upstream);
			inner.subscribe(subscriber);
		}

		@Actor
		public void sendNext() {
			inner.sendNext(1L);
			inner.sendNext(2L);
			inner.sendNext(3L);
			inner.sendNext(4L);
			inner.sendNext(5L);
			inner.sendComplete();
		}

		@Actor
		public void sendRequest() {
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
		}

		@Actor
		public void sendCancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get() + subscriber.onNextDiscarded.get();
			result.r2 = subscriber.onCompleteCalls.get() + subscriber.onErrorCalls.get() * 2L;
			result.r3 = FluxWindowTimeout.InnerWindow.isCancelled(inner.state);

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext");
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete");
			}

			if ( subscriber.onNextCalls.get() + subscriber.onNextDiscarded.get() != 5) {
				throw new IllegalStateException(parent.signals.toString());
			}

		}
	}

	@JCStressTest
	@Outcome(id = {"5, 1, true"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"5, 1, false"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutInnerWindowStressTest2 {

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
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false);

		{
			parent.onSubscribe(upstream);
			inner.subscribe(subscriber);
		}

		@Actor
		public void sendNext() {
			inner.sendNext(1L);
			inner.sendNext(2L);
			inner.sendNext(3L);
			inner.sendNext(4L);
			inner.sendNext(5L);
			inner.sendComplete();
		}

		@Actor
		public void sendRequest() {
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
			subscriber.request(1L);
		}

		@Actor
		public void sendCancel() {
			inner.sendCancel();
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get() + subscriber.onNextDiscarded.get();
			result.r2 = subscriber.onCompleteCalls.get() + subscriber.onErrorCalls.get() * 2L;
			result.r3 = FluxWindowTimeout.InnerWindow.isCancelled(inner.state);

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext");
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete");
			}

			if ( subscriber.onNextCalls.get() + subscriber.onNextDiscarded.get() != 5) {
				throw new IllegalStateException(result.r1 + parent.signals.toString());
			}
		}
	}

}

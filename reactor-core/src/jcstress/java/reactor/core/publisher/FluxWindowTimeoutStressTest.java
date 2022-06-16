/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import reactor.core.util.FastLogger;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.concurrent.Queues;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

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
		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest1_0");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + "\n" + fastLogger);
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest1_1");
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
						virtualTimeScheduler,
						new StateLogger(fastLogger)
						);
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + "\n" + fastLogger);
			}

			if (result.toString().startsWith("2, 1, 2")) {
				throw new IllegalStateException("boom " + result + "\n" + fastLogger);
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
	@Outcome(id = {"2, 4, 4, 1, 4"}, expect = ACCEPTABLE, desc = "")
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
		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest1_2");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
		public void advanceTime() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void requestMain() {
			mainSubscriber.request(1);
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() != 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + "\n" + fastLogger);
			}
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
		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest1_3");
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
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
			proxy.onComplete();
		}

		@Actor
		public void advanceTime() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void requestMain() {
			mainSubscriber.request(1);
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}
			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + mainSubscriber.onCompleteCalls.get() + "\n" + fastLogger);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"2, 0, 1, 1, 2"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 0, 1, 1, 3"}, expect = ACCEPTABLE, desc = "")
	@Outcome(id = {"2, 0, 1, 1, 4"}, expect = ACCEPTABLE, desc = "")
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
	@Outcome(id = {"2, 3, 3, 0, 4"}, expect = ACCEPTABLE, desc = "")
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest2_0");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
		public void advanceTime() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void cancel() {
			subscriber1.cancel();
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("Multiple completions" + "\n" + fastLogger);
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest2_1");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
		public void advanceTime() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void cancel() {
			subscriber2.cancel();
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("Multiple completions");
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutStressTest2_2");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long>
		                                   windowTimeoutSubscriber =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						mainSubscriber,
						2,
						1,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
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
			windowTimeoutSubscriber.onComplete();
		}

		@Actor
		public void advanceTime() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
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
				extraDiscarded++;
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
				throw new IllegalStateException("mainSubscriber Concurrent OnNext " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnNext);
			}

			if (mainSubscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("mainSubscriber Concurrent OnComplete " + result + "\n" + fastLogger, mainSubscriber.stacktraceOnComplete);
			}

			if (mainSubscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("Multiple completions" + "\n" + fastLogger);
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutInnerWindowStressTest");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> parent               =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(downstream,
						10,
						10,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
		final StressSubscription<Long>                                        upstream             =
				new StressSubscription<>(parent);
		final FluxWindowTimeout.InnerWindow<Long>                             inner                =
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false, parent.logger);

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

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext" + "\n" + fastLogger);
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete" + "\n" + fastLogger);
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutInnerWindowStressTest1");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> parent               =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(downstream,
						10,
						10,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
		final StressSubscription<Long>                                        upstream             =
				new StressSubscription<>(parent);
		final FluxWindowTimeout.InnerWindow<Long>                             inner                =
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false, parent.logger);

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
				throw new RuntimeException("concurrentOnNext" + "\n" + fastLogger);
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete" + "\n" + fastLogger);
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

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutInnerWindowStressTest2");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> parent               =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(downstream,
						10,
						10,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
		final StressSubscription<Long>                                        upstream             =
				new StressSubscription<>(parent);
		final FluxWindowTimeout.InnerWindow<Long>                             inner                =
				new FluxWindowTimeout.InnerWindow<>(10, parent, 1, false, parent.logger);

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
				throw new RuntimeException("concurrentOnNext" + "\n" + fastLogger);
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete" + "\n" + fastLogger);
			}
		}
	}



	@JCStressTest
	@Outcome(id = {"5, 1"}, expect = ACCEPTABLE, desc = "")
	@State
	public static class FluxWindowTimoutInnerWindowStressTest3 {

		final VirtualTimeScheduler                                            virtualTimeScheduler =
				VirtualTimeScheduler.create();
		final StressSubscriber<Flux<Long>>                                    downstream           =
				new StressSubscriber<>(0);
		final StressSubscriber<Long>                                          subscriber           =
				new StressSubscriber<>(0);

		final FastLogger           fastLogger              =
				new FastLogger("FluxWindowTimoutInnerWindowStressTest3");
		final FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Long> parent               =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(downstream,
						10,
						10,
						TimeUnit.SECONDS,
						virtualTimeScheduler,
						new StateLogger(fastLogger));
		final StressSubscription<Long>                                        upstream             =
				new StressSubscription<>(parent);
		final FluxWindowTimeout.InnerWindow<Long>                             inner                =
				new FluxWindowTimeout.InnerWindow<>(5, parent, 1, false, parent.logger);

		{
			parent.onSubscribe(upstream);
			parent.window = inner;
			inner.subscribe(subscriber);
		}


		int delivered;

		@Actor
		public void sendNext() {
			delivered = inner.sendNext(1L) ? delivered : (delivered + 1) ;
			delivered = inner.sendNext(2L) ? delivered : (delivered + 1) ;
			delivered = inner.sendNext(3L) ? delivered : (delivered + 1) ;
			delivered = inner.sendNext(4L) ? delivered : (delivered + 1) ;
			delivered = inner.sendNext(5L) ? delivered : (delivered + 1) ;
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
		public void sendComplete() {
			inner.run();
		}

		@Arbiter
		public void arbiter(LL_Result result) {
			result.r1 = subscriber.onNextCalls.get() + delivered;
			result.r2 = subscriber.onCompleteCalls.get() + subscriber.onErrorCalls.get() * 2L;;

			if (subscriber.concurrentOnNext.get()) {
				throw new RuntimeException("concurrentOnNext" + "\n" + fastLogger);
			}

			if (subscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("concurrentOnComplete" + "\n" + fastLogger);
			}
		}
	}

}

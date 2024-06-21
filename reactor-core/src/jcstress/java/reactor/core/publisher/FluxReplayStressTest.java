/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIIII_Result;
import org.openjdk.jcstress.infra.results.IIIIII_Result;
import org.openjdk.jcstress.infra.results.JJ_Result;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

public abstract class FluxReplayStressTest {

	public abstract static class RefCntConcurrentSubscriptionBaseStressTest<T> {

		final Flux<T> sharedSource;

		final StressSubscriber<T> subscriber1 = new StressSubscriber<>();
		final StressSubscriber<T> subscriber2 = new StressSubscriber<>();

		public RefCntConcurrentSubscriptionBaseStressTest(Flux<T> sourceToShare) {
			this(sourceToShare, null);
		}

		public RefCntConcurrentSubscriptionBaseStressTest(Flux<T> sourceToShare,
				@Nullable Duration duration) {
			this(sourceToShare, 2, duration);
		}

		public RefCntConcurrentSubscriptionBaseStressTest(Flux<T> sourceToShare,
				int subscribersCnt,
				@Nullable Duration duration) {
			if (duration == null) {
				this.sharedSource = sourceToShare.replay(1)
				                                 .refCount(subscribersCnt);
			}
			else {
				this.sharedSource = sourceToShare.replay(1)
				                                 .refCount(subscribersCnt, duration);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"[0,1], 1, [0,1], 1, 0, 0, [1,2]"}, expect = ACCEPTABLE, desc =
			"Normal scenario when cancellation of the first subscriber has no effect on" +
					" the second. The second subscription may still take place since" +
					" the  last subscriber can join the first subscription. However, due" +
					" to natural concurrency, when the synchronization block is entered," +
					" the connection is nulled. This will cause connect() to be called." +
					" Current ConnectableFlux api does not allow any improvements on that" +
					" front since it lacks coordination.")
	@Outcome(id = {"[0,1], 1, [0,1], 0, 0, 1, [1,2]"}, expect = ACCEPTABLE_INTERESTING, desc =
			"Expected corner case when the second subscriber still joins the first" +
					" subscription, but due to natural concurrency, cancellation" +
					" happens before onComplete is called. So the second subscriber gets the value" +
					" and onError instead of onComplete. The second connect call may still" +
					" happen, since ConnectableFlux.subscribe happens before the check of" +
					" the current connection value in FluxRefCnt")
	@Outcome(id = {"0, 0, 0, 0, 0, 1, 2"}, expect = ACCEPTABLE_INTERESTING, desc =
			"Expected corner case when the second subscriber still joins the first" +
					" subscription, but due to natural concurrency, cancellation of the" +
					" first subscriber happens before the value is delivered. In that case" +
					" onError is delivered instead of any values")
	@State
	public static class RefCntRaceSubscribeAndCancelNoTimeoutStressTest {

		final LongAdder     sourceSubscriptionsCnt = new LongAdder();
		final Flux<Integer> sharedSource;

		final StressSubscriber<Integer> subscriber1 = new StressSubscriber<>();
		final StressSubscriber<Integer> subscriber2 = new StressSubscriber<>();

		public RefCntRaceSubscribeAndCancelNoTimeoutStressTest() {
			this.sharedSource = Flux.range(0, 1)
			                        .doOnSubscribe(s -> sourceSubscriptionsCnt.increment())
			                        .replay(1)
			                        .refCount(1, Duration.ofMillis(0));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

		@Actor
		public void cancel() {
			subscriber1.cancel();
		}

		@Arbiter
		public void arbiter(IIIIIII_Result r) {
			r.r1 = subscriber1.onNextCalls.get();
			r.r2 = subscriber2.onNextCalls.get();
			r.r3 = subscriber1.onCompleteCalls.get();
			r.r4 = subscriber2.onCompleteCalls.get();
			r.r5 = subscriber1.onErrorCalls.get();
			r.r6 = subscriber2.onErrorCalls.get();
			r.r7 = sourceSubscriptionsCnt.intValue();
		}
	}

	@JCStressTest
	@Outcome(id = {"100000, 100000"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntRaceManySubscribersSubscribeAndCancelNoTimeoutStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		final LongAdder signalCount1 = new LongAdder();
		final LongAdder signalCount2 = new LongAdder();

		public RefCntRaceManySubscribersSubscribeAndCancelNoTimeoutStressTest() {
			super(Flux.range(0, 1), 1, Duration.ofMillis(0));
		}

		/*
		Thread 1                                 Thread 2
		subscribe(s11)                           subscribe(s21)
		        |                                       |
	    source.subscribe                         source.subscribe
        new FR connection 1 is created           we are added to existing FR connection 1
        FRCG counter + 1 = 1                            |
        connect()                                       |
        value is delivered                              |
        MonoNext calls cancel                           |
        enter sync monitor                              |
        FRCG count - 1 = 0                              |
        FRCG.connection = null                          |
        exit sync monitor                               |
                |                                the subscriber21 is added to the list of FR
        disposable.dispose()                            |
                |                                enters sync monitor
                |                                FRCG connection is null
                |                                create new FRCG connection
                |                                FRCG counter + 1 = 1
                |                                connect()
                |                                new FR connection 2 is created
                |                                subscription is started
       we see subscriber21 in the list                  |
       we deliver value to subscriber21                 |
                                                        |
       MonoNext calls cancel                            |
       enter sync monitor                               |
                |                                       |
       FRCG count - 1 = 0                               |
       FRCG.connection2 = null                          |
       connection2.dispose()                            |
       cancel subscription                              |
                |                                we see subscription is canceled
                |                                so we return without any value
                |                                being delivered
                |                                subscribe(s22)
                |                                        |
                |                                FRCG.source.subscribe(s22)
                |                                subscriber s22 is added to FR.connection2
       CONNECTION.lazySet(null)
       terminate()
       we see subscriber22 in the list
       we send error to subscriber22
	   */

		@Actor
		public void subscribe1() {
			CountDownLatch latch = new CountDownLatch(100_000);
			for (int i = 0; i < 100_000; i++) {
				sharedSource.next()
				            .subscribe(new BaseSubscriber<Integer>() {
					            @Override
					            protected void hookOnNext(Integer value) {
						            signalCount1.increment();
					            }

					            @Override
					            protected void hookFinally(SignalType type) {
						            latch.countDown();
					            }
				            });
			}
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Actor
		public void subscribe2() {
			CountDownLatch latch = new CountDownLatch(100_000);
			for (int i = 0; i < 100_000; i++) {
				sharedSource.next()
				            .subscribe(new BaseSubscriber<Integer>() {
					            @Override
					            protected void hookOnNext(Integer value) {
						            signalCount2.increment();
					            }

					            @Override
					            protected void hookFinally(SignalType type) {
						            latch.countDown();
					            }
				            });
			}
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Arbiter
		public void arbiter(JJ_Result r) {
			r.r1 = signalCount1.sum();
			r.r2 = signalCount2.sum();
		}
	}

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionRangeSyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntConcurrentSubscriptionRangeSyncFusionStressTest() {
			super(Flux.range(0, 10));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionRangeAsyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntConcurrentSubscriptionRangeAsyncFusionStressTest() {
			super(Flux.range(0, 10).publishOn(Schedulers.immediate()));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionRangeNoneFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntConcurrentSubscriptionRangeNoneFusionStressTest() {
			super(Flux.range(0, 10).hide());
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionEmptySyncStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntConcurrentSubscriptionEmptySyncStressTest() {
			super(Flux.empty());
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionEmptyAsyncStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntConcurrentSubscriptionEmptyAsyncStressTest() {
			super(Flux.empty().publishOn(Schedulers.immediate()));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionEmptyNoneStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntConcurrentSubscriptionEmptyNoneStressTest() {
			super(Flux.empty().hide());
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionErrorSyncStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntConcurrentSubscriptionErrorSyncStressTest() {
			super(Flux.error(testError));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionErrorAsyncStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntConcurrentSubscriptionErrorAsyncStressTest() {
			super(Flux.error(testError).publishOn(Schedulers.immediate()));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionErrorNoneStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntConcurrentSubscriptionErrorNoneStressTest() {
			super(Flux.error(testError).hide());
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionRangeAsyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntGraceConcurrentSubscriptionRangeAsyncFusionStressTest() {
			super(Flux.range(0, 10).publishOn(Schedulers.immediate()), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionRangeSyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntGraceConcurrentSubscriptionRangeSyncFusionStressTest() {
			super(Flux.range(0, 10), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionRangeNoneFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntGraceConcurrentSubscriptionRangeNoneFusionStressTest() {
			super(Flux.range(0, 10).hide(), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {
			"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionEmptyAsyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntGraceConcurrentSubscriptionEmptyAsyncFusionStressTest() {
			super(Flux.empty().publishOn(Schedulers.immediate()), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionEmptyNoneFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntGraceConcurrentSubscriptionEmptyNoneFusionStressTest() {
			super(Flux.empty().hide(), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionEmptySyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntGraceConcurrentSubscriptionEmptySyncFusionStressTest() {
			super(Flux.empty(), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionErrorAsyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntGraceConcurrentSubscriptionErrorAsyncFusionStressTest() {
			super(Flux.error(testError).publishOn(Schedulers.immediate()), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionErrorSyncFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntGraceConcurrentSubscriptionErrorSyncFusionStressTest() {
			super(Flux.error(testError), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

	@JCStressTest
	@Outcome(id = {"0, 0, 0, 0, 1, 1"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionErrorNoneFusionStressTest
			extends RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntGraceConcurrentSubscriptionErrorNoneFusionStressTest() {
			super(Flux.error(testError).hide(), Duration.ofSeconds(1));
		}

		@Actor
		public void subscribe1() {
			sharedSource.subscribe(subscriber1);
		}

		@Actor
		public void subscribe2() {
			sharedSource.subscribe(subscriber2);
		}

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

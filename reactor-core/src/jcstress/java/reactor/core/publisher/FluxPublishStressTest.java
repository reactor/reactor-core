/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIII_Result;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxPublishStressTest {

	public abstract static class RefCntConcurrentSubscriptionBaseStressTest<T> {

		final Flux<T> sharedSource;

		final StressSubscriber<T> subscriber1 = new StressSubscriber<>();
		final StressSubscriber<T> subscriber2 = new StressSubscriber<>();

		public RefCntConcurrentSubscriptionBaseStressTest(Flux<T> sourceToShare, @Nullable Duration duration) {
			if (duration == null) {
				this.sharedSource = sourceToShare.publish()
				                                 .refCount(2);
			}
			else {
				this.sharedSource = sourceToShare.publish()
				                                 .refCount(2, duration);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"10, 10, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntConcurrentSubscriptionRangeAsyncFusionStressTest extends
	                                                                           RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntConcurrentSubscriptionRangeAsyncFusionStressTest() {
			super(Flux.range(0, 10).publishOn(Schedulers.immediate()), null);
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
	public static class RefCntConcurrentSubscriptionRangeNoneFusionStressTest extends
	                                                                          RefCntConcurrentSubscriptionBaseStressTest<Integer> {

		public RefCntConcurrentSubscriptionRangeNoneFusionStressTest() {
			super(Flux.range(0, 10).hide(), null);
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
	public static class RefCntConcurrentSubscriptionEmptyAsyncStressTest extends
	                                                                     RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntConcurrentSubscriptionEmptyAsyncStressTest() {
			super(Flux.empty().publishOn(Schedulers.immediate()), null);
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
	public static class RefCntConcurrentSubscriptionEmptyNoneStressTest extends
	                                                                    RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntConcurrentSubscriptionEmptyNoneStressTest() {
			super(Flux.empty().hide(), null);
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
	public static class RefCntConcurrentSubscriptionErrorAsyncStressTest extends
	                                                                     RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntConcurrentSubscriptionErrorAsyncStressTest() {
			super(Flux.error(testError).publishOn(Schedulers.immediate()), null);
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
	public static class RefCntConcurrentSubscriptionErrorNoneStressTest extends
	                                                                    RefCntConcurrentSubscriptionBaseStressTest<Object> {

		static final Throwable testError = new RuntimeException("boom");

		public RefCntConcurrentSubscriptionErrorNoneStressTest() {
			super(Flux.error(testError).hide(), null);
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
	public static class RefCntGraceConcurrentSubscriptionRangeAsyncFusionStressTest extends
	                                                                                RefCntConcurrentSubscriptionBaseStressTest<Integer> {

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
	public static class RefCntGraceConcurrentSubscriptionRangeNoneFusionStressTest extends
	                                                                    RefCntConcurrentSubscriptionBaseStressTest<Integer> {

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
	@Outcome(id = {"0, 0, 1, 1, 0, 0"}, expect = ACCEPTABLE, desc = "concurrent subscription succeeded")
	@State
	public static class RefCntGraceConcurrentSubscriptionEmptyAsyncStressTest extends
	                                                               RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntGraceConcurrentSubscriptionEmptyAsyncStressTest() {
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
	public static class RefCntGraceConcurrentSubscriptionEmptyNoneStressTest extends
	                                                              RefCntConcurrentSubscriptionBaseStressTest<Object> {

		public RefCntGraceConcurrentSubscriptionEmptyNoneStressTest() {
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
}

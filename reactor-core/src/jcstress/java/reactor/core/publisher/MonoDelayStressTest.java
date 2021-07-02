/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.TimeUnit;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

public abstract class MonoDelayStressTest {

	private static final int REQUEST_BEFORE_TICK                          = 0b0011_0_111; // 55
	private static final int REQUEST_AFTER_TICK                           = 0b0010_0_111; // 39
	private static final int CANCELLED_AFTER_REQUEST_FIRST                = 0b0111_0_001; //113
	private static final int CANCELLED_AFTER_REQUEST_FIRST_AND_DELAY_DONE = 0b0111_0_011; //115
	private static final int CANCELLED_AFTER_REQUEST_SECOND               = 0b0110_0_011; // 99
	private static final int CANCELLED_BEFORE_REQUEST_BUT_DELAY_DONE      = 0b0100_0_011; // 67
	private static final int CANCELLED_EARLY                              = 0b0100_0_001; // 65
	private static final int CANCELLED_SUPER_EARLY                        = 0b0100_0_000; // 64

	@JCStressTest
	@Outcome(id = {"1, 0, " + REQUEST_BEFORE_TICK}, expect = ACCEPTABLE, desc = "Request before tick was delivered")
	@Outcome(id = {"1, 0, " + REQUEST_AFTER_TICK}, expect = ACCEPTABLE, desc = "Request AFTER tick was delivered")
	@State
	public static class RequestAndRunStressTest {

		/*
		Implementation notes: in this test we use the VirtualTimeScheduler to better coordinate
		the triggering of the `run` method. We directly interpret the end STATE to track what
		happened.
		 */

		final StressSubscriber<Long> subscriber = new StressSubscriber<>(0L);
		final VirtualTimeScheduler   virtualTimeScheduler;
		final MonoDelay              monoDelay;

		MonoDelay.MonoDelayRunnable subscription;

		{
			virtualTimeScheduler = VirtualTimeScheduler.create();
			monoDelay = new MonoDelay(Long.MAX_VALUE, TimeUnit.MILLISECONDS, virtualTimeScheduler);
			monoDelay.doOnSubscribe(s -> subscription = (MonoDelay.MonoDelayRunnable) s).subscribe(subscriber);
		}

		@Actor
		public void delayTrigger() {
			subscription.run();
		}

		@Actor
		public void request() {
			subscriber.request(1);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onErrorCalls.get();
			r.r3 = subscription.state;
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 0, " + REQUEST_BEFORE_TICK}, expect = ACCEPTABLE, desc = "Tick was delivered, request happened before tick")
	@Outcome(id = {"1, 0, " + REQUEST_AFTER_TICK}, expect = ACCEPTABLE, desc = "Tick was delivered, request happened after tick")
	@Outcome(id = {"0, 0, " + CANCELLED_AFTER_REQUEST_FIRST}, expect = ACCEPTABLE, desc = "Cancelled after request, tick not done yet")
	@Outcome(id = {"0, 0, " + CANCELLED_AFTER_REQUEST_FIRST_AND_DELAY_DONE}, expect = ACCEPTABLE, desc = "Cancelled after (request then tick)")
	@Outcome(id = {"0, 0, " + CANCELLED_AFTER_REQUEST_SECOND}, expect = ACCEPTABLE, desc = "Cancelled after (tick then request)")
	@Outcome(id = {"0, 0, " + CANCELLED_BEFORE_REQUEST_BUT_DELAY_DONE}, expect = ACCEPTABLE, desc = "Cancelled before request, but after tick happened")
	@Outcome(id = {"0, 0, " + CANCELLED_EARLY}, expect = ACCEPTABLE, desc = "Cancelled before request and tick")
	@Outcome(id = {"0, 0, " + CANCELLED_SUPER_EARLY}, expect = ACCEPTABLE_INTERESTING, desc = "Cancelled before even setCancel")
	@State
	public static class RequestAndCancelStressTest {

		/*
		Implementation notes: in this test we use the VirtualTimeScheduler to better coordinate
		the triggering of the `run` method. We directly interpret STATE to track what happened.
		 */

		final StressSubscriber<Long>                       subscriber      = new StressSubscriber<>(0L);
		final VirtualTimeScheduler                         virtualTimeScheduler;
		final MonoDelay                                    monoDelay;
		MonoDelay.MonoDelayRunnable subscription;

		{
			virtualTimeScheduler = VirtualTimeScheduler.create();
			monoDelay = new MonoDelay(Long.MAX_VALUE, TimeUnit.MILLISECONDS, virtualTimeScheduler);
			monoDelay
					.doOnSubscribe(s -> subscription = (MonoDelay.MonoDelayRunnable) s)
					.subscribe(subscriber);
		}

		@Actor
		public void request() {
			subscriber.request(1);
		}

		@Actor
		public void delayTrigger() {
			subscription.run();
		}

		@Actor
		public void cancelFromActual() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onErrorCalls.get();
			r.r3 = subscription.state;
		}
	}
}

/*
 * Copyright (c) 2021-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

@SuppressWarnings("unchecked")
public abstract class MonoDelayElementStressTest {

	@JCStressTest
	@Outcome(id = {"1, 0, 1"}, expect = ACCEPTABLE)
	@State
	public static class OnNextAndRunStressTest {

		/*
		Implementation notes: in this test we use the VirtualTimeScheduler to better coordinate
		the triggering of the `run` method
		 */

		final StressSubscriber<Object> subscriber = new StressSubscriber<>(0L);
		final VirtualTimeScheduler     virtualTimeScheduler;
		final MonoDelayElement<Object> monoDelay;

		MonoDelayElement.DelayElementSubscriber<Object> subscription;

		{
			virtualTimeScheduler = VirtualTimeScheduler.create();
			monoDelay = new MonoDelayElement<>(Mono.never(), 0L,
					TimeUnit.MILLISECONDS,
					virtualTimeScheduler);

			monoDelay.doOnSubscribe(s -> subscription =
					((MonoDelayElement.DelayElementSubscriber<Object>) s)).subscribe(subscriber);
		}

		@Actor
		public void delayTrigger() {
			subscription.onNext(1);
		}

		@Actor
		public void request() {
			virtualTimeScheduler.advanceTime();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			virtualTimeScheduler.advanceTime();
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onErrorCalls.get();
			r.r3 = subscriber.onCompleteCalls.get();
		}
	}
}

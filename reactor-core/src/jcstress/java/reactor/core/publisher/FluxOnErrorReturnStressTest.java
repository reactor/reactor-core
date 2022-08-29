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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIII_Result;
import org.openjdk.jcstress.infra.results.IIII_Result;
import org.openjdk.jcstress.infra.results.III_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public abstract class FluxOnErrorReturnStressTest {

	@JCStressTest
	@Outcome(id = {"1, 1, 0, 1, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r1")
	@Outcome(id = {"1, 1, 0, 2, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r2")
	@Outcome(id = {"1, 1, 0, 3, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r3 or r1+r2")
	@Outcome(id = {"1, 1, 0, 4, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r1+r3")
	@Outcome(id = {"1, 1, 0, 5, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r2+r3")
	@Outcome(id = {"1, 1, 0, 6, 0"}, expect = ACCEPTABLE, desc = "error triggered fallback after r1+r2+r3")
	@Outcome(id = {"1, 1, 0, 1, 1"}, expect = ACCEPTABLE, desc = "deferred fallback until request1")
	@Outcome(id = {"1, 1, 0, 2, 1"}, expect = ACCEPTABLE, desc = "deferred fallback until request2")
	@Outcome(id = {"1, 1, 0, 3, 1"}, expect = ACCEPTABLE, desc = "deferred fallback until request3")
	@Outcome(id = {"1, 1, 0, 0, 0"}, expect = FORBIDDEN, desc = "fallback sent despite no demand")
	@State
	public static class ErrorFallbackVsRequestStressTest {

		final Throwable ERROR = new IllegalStateException("expected");

		final StressSubscriber<Integer>                   subscriber = new StressSubscriber<>(0L);
		final FluxOnErrorReturn.ReturnSubscriber<Integer> test = new FluxOnErrorReturn.ReturnSubscriber<>(subscriber, null, 100, true);
		final StressSubscription<Integer>                 topmost = new StressSubscription<>(test);

		{
			test.onSubscribe(topmost);
		}

		@Actor
		public void error() {
			test.onError(ERROR);
		}

		@Actor
		public void request1() {
			subscriber.request(1);
		}

		@Actor
		public void request2() {
			subscriber.request(2);
		}

		@Actor
		public void request3() {
			subscriber.request(3);
		}

		@Arbiter
		public void arbiter(IIIII_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onErrorCalls.get();
			r.r4 = (int) topmost.requested;
			r.r5 = topmost.cancelled.get() ? 1 : 0;
		}
	}
}
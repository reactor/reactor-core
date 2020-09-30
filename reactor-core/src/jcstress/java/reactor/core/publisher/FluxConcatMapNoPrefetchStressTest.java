/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.ZZ_Result;
import reactor.core.publisher.FluxConcatMapNoPrefetch.FluxConcatMapNoPrefetchSubscriber;

import static org.openjdk.jcstress.annotations.Expect.*;

public abstract class FluxConcatMapNoPrefetchStressTest {

	final StressSubscriber<Object> stressSubscriber = new StressSubscriber<>();

	final FluxConcatMapNoPrefetchSubscriber<Object, Object> concatMapImmediate = new FluxConcatMapNoPrefetchSubscriber<>(
			stressSubscriber,
			Mono::just,
			FluxConcatMap.ErrorMode.IMMEDIATE
	);

	@JCStressTest
	@Outcome(id = {"false, false"}, expect = ACCEPTABLE,  desc = "No concurrent invocations")
	@Outcome(id = {"true, false"}, expect = FORBIDDEN,  desc = "onNext while onError")
	@Outcome(id = {"false, true"}, expect = FORBIDDEN,  desc = "onError while onNext")
	@State
	public static class OnErrorStressTest extends FluxConcatMapNoPrefetchStressTest {

		{
			concatMapImmediate.state = FluxConcatMapNoPrefetchSubscriber.State.ACTIVE;
		}

		@Actor
		public void inner() {
			concatMapImmediate.innerNext("hello");
		}

		@Actor
		public void outer() {
			concatMapImmediate.onError(new RuntimeException("Boom!"));
		}

		@Arbiter
		public void arbiter(ZZ_Result r) {
			r.r1 = stressSubscriber.concurrentOnNext.get();
			r.r2 = stressSubscriber.concurrentOnError.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"1"}, expect = ACCEPTABLE, desc = "Exactly one onComplete")
	@State
	public static class OnCompleteStressTest extends FluxConcatMapNoPrefetchStressTest {

		{
			concatMapImmediate.state = FluxConcatMapNoPrefetchSubscriber.State.LAST_ACTIVE;
		}

		@Actor
		public void inner() {
			concatMapImmediate.innerComplete();
		}

		@Actor
		public void outer() {
			concatMapImmediate.onComplete();
		}

		@Arbiter
		public void arbiter(I_Result r) {
			r.r1 = stressSubscriber.onCompleteCalls.get();
		}
	}
}
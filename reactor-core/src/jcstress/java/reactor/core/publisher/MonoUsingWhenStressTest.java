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

import java.util.concurrent.atomic.AtomicIntegerArray;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLL_Result;

import static org.openjdk.jcstress.annotations.Expect.*;

public abstract class MonoUsingWhenStressTest {

	//SENT, DERIVED, CLOSED, CANCELLED, VALUE OBSERVED
	@JCStressTest
	@Outcome(id = {"OK, 1, 1, 0, 100"}, expect = FORBIDDEN, desc = "generated, derived, closed but no emit observed")
	@Outcome(id = {"OK, 0, 4040, 0, 0"}, expect = FORBIDDEN, desc = "generated, but otherwise unprocessed")
	@Outcome(id = {"OK, 1, 1, 0, 101"}, expect = ACCEPTABLE, desc = "generated, derived, closed after emitted")
	@Outcome(id = {"OK, 1, 0, 1, 0"}, expect = ACCEPTABLE, desc = "generated, derived, cancelled before emit observed")
	@Outcome(id = {"CANCELLED, 0, 0, 0, 0"}, expect = ACCEPTABLE, desc = "cancelled before generation")
	@Outcome(id = {"CANCELLED, 0, 1, 0, 0"}, expect = ACCEPTABLE_INTERESTING, desc = "cancelled before generation, but closed")
	@State
	public static class CancelCloseToResourceEmission {

		final AtomicIntegerArray producedAndTerminatedOrCleaned = new AtomicIntegerArray(3);
		Sinks.One<AtomicIntegerArray> resourceSink = Sinks.one();

		final Mono<Long> mono = Mono.usingWhen(
			resourceSink.asMono(),
			it -> {
				it.incrementAndGet(0);
				return Mono.never();
			},
			it -> Mono.fromRunnable(() -> it.incrementAndGet(1)),
			(it, error) -> Mono.fromRunnable(() -> it.incrementAndGet(1)),
			it -> Mono.fromRunnable(() -> it.incrementAndGet(2))
		);

		final StressSubscriber<Long> valueSubscriber = new StressSubscriber<>(100);

		{
			mono.subscribe(valueSubscriber);
		}

		@Actor
		public void emit(LLLLL_Result result) {
			result.r1 = resourceSink.tryEmitValue(producedAndTerminatedOrCleaned);
		}

		@Actor
		public void cancel() {
			valueSubscriber.cancel();
		}

		@Actor
		public void waitForTermination() {
			long deadline = System.currentTimeMillis() + 500;
			while (true) {
				if (System.currentTimeMillis() > deadline) {
					producedAndTerminatedOrCleaned.addAndGet(1, 4040);
					return;
				}
				if (producedAndTerminatedOrCleaned.get(1) != 0 ||
					producedAndTerminatedOrCleaned.get(2) != 0) {
					return;
				}
				try {
					Thread.sleep(50);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r2 = producedAndTerminatedOrCleaned.get(0);
			result.r3 = producedAndTerminatedOrCleaned.get(1);
			result.r4 = producedAndTerminatedOrCleaned.get(2);
			result.r5 = valueSubscriber.onNextCalls.get() +
				valueSubscriber.onErrorCalls.get() * 100 +
				valueSubscriber.onCompleteCalls.get() * 1000;
		}
	}
}

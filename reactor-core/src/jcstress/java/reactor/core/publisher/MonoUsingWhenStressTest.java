/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import org.openjdk.jcstress.infra.results.LLLL_Result;
import reactor.core.CoreSubscriber;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class MonoUsingWhenStressTest {

	//SENT, DERIVED, CLOSED, CANCELLED, VALUE OBSERVED
	@JCStressTest
	@Outcome(id = {"1, 0, 1, 0"}, expect = ACCEPTABLE, desc = "released")
	@Outcome(id = {"0, 0, 0, 0"}, expect = ACCEPTABLE, desc = "not delivered")
	@Outcome(id = {"1, 1, 0, 1000"}, expect = ACCEPTABLE, desc = "delivered and completed")
	@State
	public static class CancelCloseToResourceEmission {

		final AtomicIntegerArray producedAndTerminatedOrCleaned = new AtomicIntegerArray(3);
		StressSubscription<? super AtomicIntegerArray> resourceSink;

		final Mono<Long> mono = Mono.usingWhen(
			new Mono<AtomicIntegerArray>() {
				@Override
				public void subscribe(CoreSubscriber<? super AtomicIntegerArray> actual) {
					resourceSink = new StressSubscription<>(actual);
					actual.onSubscribe(resourceSink);
				}
			},
			it -> {
				it.incrementAndGet(0);
				return Mono.empty();
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
		public void emit() {
			if (resourceSink.cancelled.get()) {
				return;
			}

			resourceSink.actual.onNext(producedAndTerminatedOrCleaned);
			resourceSink.actual.onComplete();
		}

		@Actor
		public void cancel() {
			valueSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLL_Result result) {
			result.r1 = producedAndTerminatedOrCleaned.get(0);
			result.r2 = producedAndTerminatedOrCleaned.get(1);
			result.r3 = producedAndTerminatedOrCleaned.get(2);
			result.r4 = valueSubscriber.onNextCalls.get() +
				valueSubscriber.onErrorCalls.get() * 100 +
				valueSubscriber.onCompleteCalls.get() * 1000;
		}
	}
}

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

import java.util.concurrent.atomic.AtomicReference;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import org.reactivestreams.Subscriber;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public class FluxMergeSequentialStressTest {
	@JCStressTest
	@Outcome(id = {"1, 0"}, expect = ACCEPTABLE, desc = "onError shortCircuited onComplete")
	@Outcome(id = {"0, 1"}, expect = FORBIDDEN, desc = "onComplete shortCircuited onError")
	@State
	public static class MergeSequentialCompleteErrorStressTest {

		final AtomicReference<Subscriber<? super Integer>> actual1 = new AtomicReference<>();
		final AtomicReference<Subscriber<? super Integer>> actual2 = new AtomicReference<>();

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);

		{
			final Flux<Integer> merged = Flux.mergeSequential(actual -> {
				actual1.set(actual);
				actual.onSubscribe(Operators.emptySubscription());
			}, actual -> {
				actual2.set(actual);
				actual.onSubscribe(Operators.emptySubscription());
			});
			merged.subscribe(subscriber);
		}

		@Actor
		public void completeOne() {
			actual1.get().onComplete();
		}

		@Actor
		public void errorTwo() {
			actual2.get().onError(new IllegalStateException("boom"));
		}

		@Arbiter
		public void arbiter(II_Result r) {
			r.r1 = subscriber.onErrorCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
		}
	}
}

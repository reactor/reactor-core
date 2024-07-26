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

import java.util.concurrent.ForkJoinPool;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class FluxPublishOnStressTest {

	@JCStressTest
	@Outcome(id = {"0, 1", "0, 0"}, expect = ACCEPTABLE, desc = "no errors propagated after cancellation because of disposed worker")
	@State
	public static class FluxPublishOnOnNextAndCancelRaceStressTest {
		final StressSubscription<Integer> upstream = new StressSubscription<>(null);
		final StressSubscriber<Integer> downstream = new StressSubscriber<>();
		final Scheduler scheduler =
				Schedulers.fromExecutorService(ForkJoinPool.commonPool());

		final FluxPublishOn.PublishOnSubscriber<Integer> publishOnSubscriber =
				new FluxPublishOn.PublishOnSubscriber<>(downstream,
						scheduler,
						scheduler.createWorker(), true, 32, 12, Queues.get(32));


		{
			publishOnSubscriber.onSubscribe(upstream);
		}

		@Actor
		public void produce() {
			publishOnSubscriber.onNext(1);
			publishOnSubscriber.onNext(2);
			publishOnSubscriber.onNext(3);
			publishOnSubscriber.onNext(4);
		}

		@Actor
		public void cancel() {
			publishOnSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result result) {
			result.r1 = downstream.onErrorCalls.get();
			result.r2 = downstream.droppedErrors.size();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, 1", "0, 0"}, expect = ACCEPTABLE, desc = "no errors propagated after cancellation because of disposed worker")
	@State
	public static class FluxPublishOnConditionalOnNextAndCancelRaceStressTest {
		final StressSubscription<Integer> upstream = new StressSubscription<>(null);
		final ConditionalStressSubscriber<Integer> downstream = new ConditionalStressSubscriber<>();
		final Scheduler scheduler =
				Schedulers.fromExecutorService(ForkJoinPool.commonPool());

		final FluxPublishOn.PublishOnConditionalSubscriber<Integer> publishOnSubscriber =
				new FluxPublishOn.PublishOnConditionalSubscriber<>(downstream,
						scheduler,
						scheduler.createWorker(), true, 32, 12, Queues.get(32));


		{
			publishOnSubscriber.onSubscribe(upstream);
		}

		@Actor
		public void produce() {
			publishOnSubscriber.onNext(1);
			publishOnSubscriber.onNext(2);
			publishOnSubscriber.onNext(3);
			publishOnSubscriber.onNext(4);
		}

		@Actor
		public void cancel() {
			publishOnSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result result) {
			result.r1 = downstream.onErrorCalls.get();
			result.r2 = downstream.droppedErrors.size();
		}
	}
}

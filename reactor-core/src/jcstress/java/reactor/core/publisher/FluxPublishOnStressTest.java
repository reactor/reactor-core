/*
 * Copyright (c) 2021-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;
import org.reactivestreams.Subscription;

import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.openjdk.jcstress.annotations.Expect.*;

abstract class FluxPublishOnStressTest {

	@JCStressTest
	//note: -1 can still happen but should be present in a very low number
	@Outcome(id = {"-1"}, expect = ACCEPTABLE_INTERESTING, desc = "Latch timed out")
	@Outcome(id = {"1"}, expect = ACCEPTABLE,  desc = "Simple discard")
	@Outcome(id = {"10"}, expect = ACCEPTABLE,  desc = "Processed")
	@Outcome(id = {"1001"}, expect = ACCEPTABLE_INTERESTING, desc = "Discarded and onError")
	@Outcome(id = {"1010"}, expect = ACCEPTABLE_INTERESTING, desc = "Processed and onError")
	@Outcome(id = {"-2"}, expect = FORBIDDEN, desc = "Latch interrupted")
	@Outcome(id = {"0"}, expect = FORBIDDEN,  desc = "Nothing happened")
	@Outcome(id = {"2"}, expect = FORBIDDEN,  desc = "Double discard")
	@Outcome(id = {"11"}, expect = FORBIDDEN,  desc = "Processed and discarded")
	@Outcome(id = {"1000"}, expect = FORBIDDEN, desc = "only onError")
	@Outcome(id = {"1002"}, expect = FORBIDDEN, desc = "double discard and onError")
	@Outcome(id = {"1011"}, expect = FORBIDDEN, desc = "processed and discarded and onError")
	@State
	public static class QueueAndCancelDiscardStressTest extends FluxPublishOnStressTest {

		AtomicInteger           state = new AtomicInteger(0);
		CountDownLatch latch = new CountDownLatch(2);

		LambdaSubscriber<AtomicInteger> downstream = new LambdaSubscriber<>(
				ai -> {
					ai.addAndGet(10);
					latch.countDown();
				},
				t -> {
					state.addAndGet(1000);
					latch.countDown();
				},
				() -> {}, s -> s.request(1L),
				Operators.enableOnDiscard(Context.empty(), (Consumer<AtomicInteger>) ai -> {
					ai.incrementAndGet();
					latch.countDown();
				}));

		FluxPublishOn.PublishOnSubscriber<AtomicInteger> publishOnSubscriber = new FluxPublishOn.PublishOnSubscriber<>(
				downstream,
				Schedulers.immediate(), Schedulers.immediate().createWorker(), false, 10, 10,
				Queues.small());


		{
			publishOnSubscriber.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {

				}

				@Override
				public void cancel() {
				}
			});
		}

		@Actor
		void inner() {
			publishOnSubscriber.onNext(state);
		}

		@Actor
		void outer() {
			publishOnSubscriber.cancel();
			latch.countDown();
		}

		@Arbiter
		public void arbiter(I_Result r) {
			try {
				if (!latch.await(10, TimeUnit.SECONDS)) {
					r.r1 = -1;
				}
				else {
					r.r1 = state.get();
				}
			}
			catch (InterruptedException e) {
				r.r1 = -2;
			}
		}
	}
}
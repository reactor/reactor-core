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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public abstract class FluxGenerateStressTest {

	@JCStressTest
	@Outcome(id = {"100, 1, 0"}, expect = ACCEPTABLE, desc = "completed")
	@Outcome(id = {"[1-9]\\d*, 0, 1"}, expect = FORBIDDEN, desc = "hanged")
	@State
	public static class GenerateSlowPathStressTest {

		final long TIMEOUT_NANOS = Duration.ofMillis(500).toNanos();
		final int COMPLETE_SIZE = 100;

		final AtomicInteger source = new AtomicInteger();
		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(0L);

		{
			final Flux<Integer> generate = Flux.generate(sink -> {
				int i = source.incrementAndGet();
				if (i == COMPLETE_SIZE) {
					sink.next(i);
					sink.complete();
					return;
				}
				sink.next(i);
			});
			generate.subscribe(subscriber);
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

		@Actor
		public void request4() {
			subscriber.request(4);
		}

		@Actor
		public void request5() {
			subscriber.request(5);
		}

		@Actor
		public void requestRest() {
			long deadline = System.nanoTime() + TIMEOUT_NANOS;
			for (;;) {
				if (source.get() < 15) {
					if (System.nanoTime() >= deadline) {
						subscriber.onError(new IllegalStateException("timed out"));
						subscriber.subscription.cancel();
						return;
					}
				}
				else {
					break;
				}
			}

			subscriber.request(COMPLETE_SIZE);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			boolean completionTimedOut = false;
			long deadlineToComplete = System.nanoTime() + TIMEOUT_NANOS;
			for (;;) {
				if (subscriber.onCompleteCalls.get() > 0 || subscriber.onErrorCalls.get() > 0) {
					break;
				}
				if (System.nanoTime() >= deadlineToComplete) {
					completionTimedOut = true;
					break;
				}
			}

			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();

			int errors = subscriber.onErrorCalls.get();
			if (completionTimedOut) errors++;
			r.r3 = errors;
		}
	}

}
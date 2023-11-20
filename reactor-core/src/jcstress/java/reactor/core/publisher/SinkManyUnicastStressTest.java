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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

public class SinkManyUnicastStressTest {

	@JCStressTest
	@Outcome(id = {"1"}, expect = Expect.ACCEPTABLE, desc = "Item delivered")
	@State
	public static class ParallelSubscribeAndEmit {

		final Sinks.Many<Object> sink =
				Sinks.many().unicast().onBackpressureBuffer();

		final StressSubscriber<Object> subscriber = new StressSubscriber<>();

		@Actor
		public void subscribe() {
			sink.asFlux()
			    // Force request bump before the subscription is actually delivered.
			    // Otherwise, there is no race, because emitNext would not deliver due
			    // to lack of requests.
			    .doOnSubscribe(s -> s.request(Long.MAX_VALUE))
			    .subscribe(subscriber);
		}

		@Actor
		public void emit() {
			sink.tryEmitNext("Test");
		}

		@Arbiter
		public void arbiter(I_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			if (subscriber.concurrentOnSubscribe.get() || subscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("Concurrent onSubscribe with onNext");
			}
		}
	}
}

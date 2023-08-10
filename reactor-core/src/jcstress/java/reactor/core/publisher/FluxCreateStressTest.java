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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongConsumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;
import org.reactivestreams.Subscription;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxCreateStressTest {

	@JCStressTest
	@Outcome(id = {"4"}, expect = ACCEPTABLE, desc = "demand delivered")
	@State
	public static class RequestAndOnRequestStressTest implements LongConsumer {

		FluxSink<? super Integer> sink;

		Subscription s;

		volatile long observedDemand;
		static final AtomicLongFieldUpdater<RequestAndOnRequestStressTest> OBSERVED_DEMAND
				= AtomicLongFieldUpdater.newUpdater(RequestAndOnRequestStressTest.class, "observedDemand");

		{
			Flux.<Integer>create(sink -> this.sink = sink)
			    .subscribe(new BaseSubscriber<Integer>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					RequestAndOnRequestStressTest.this.s = subscription;
				}
			});
		}

		@Override
		public void accept(long value) {
			Operators.addCap(OBSERVED_DEMAND, this, value);
		}

		@Actor
		public void request() {
			s.request(1);
			s.request(1);
			s.request(1);
			s.request(1);
		}

		@Actor
		public void setOnRequestConsumer() {
			sink.onRequest(this);
		}


		@Arbiter
		public void artiber(J_Result r) {
			r.r1 = observedDemand;
		}
	}
}

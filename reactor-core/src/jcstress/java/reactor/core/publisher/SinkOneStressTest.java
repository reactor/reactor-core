/*
 * Copyright (c) 2020-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import org.openjdk.jcstress.infra.results.LLL_Result;
import org.openjdk.jcstress.infra.results.LL_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinkOneStressTest {

	@JCStressTest
	@Outcome(id = {"foo, 1"}, expect = ACCEPTABLE, desc = "value delivered")
	@State
	public static class EmitValueSubscribeStressTest extends SinkOneStressTest {

		final SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		final StressSubscriber<String> subscriber = new StressSubscriber<>();

		@Actor
		public void value() {
			sink.tryEmitValue("foo");
			sink.tryEmitEmpty();
			sink.tryEmitError(new RuntimeException());
		}

		@Actor
		public void subscriber() {
			sink.asMono().subscribe(subscriber);
		}

		@Arbiter
		public void arbiter(LL_Result r) {
			r.r1 = subscriber.onNextCalls.get() == 1 ? subscriber.receivedValues.get(0) : null;
			r.r2 = subscriber.onCompleteCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"null, 1"}, expect = ACCEPTABLE, desc = "value delivered")
	@State
	public static class EmitEmptySubscribeStressTest extends SinkOneStressTest {

		final SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		final StressSubscriber<String> subscriber = new StressSubscriber<>();

		@Actor
		public void value() {
			sink.tryEmitEmpty();
			sink.tryEmitValue("foo");
			sink.tryEmitError(new RuntimeException());
		}

		@Actor
		public void subscriber() {
			sink.asMono().subscribe(subscriber);
		}

		@Arbiter
		public void arbiter(LL_Result r) {
			r.r1 = subscriber.onNextCalls.get() == 1 ? subscriber.receivedValues.get(0) : null;
			r.r2 = subscriber.onCompleteCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"null, 0, boo"}, expect = ACCEPTABLE, desc = "value delivered")
	@State
	public static class EmitErrorSubscribeStressTest extends SinkOneStressTest {

		final SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		final StressSubscriber<String> subscriber = new StressSubscriber<>();

		@Actor
		public void value() {
			sink.tryEmitError(new RuntimeException("boo"));
			sink.tryEmitEmpty();
			sink.tryEmitValue("foo");
		}

		@Actor
		public void subscriber() {
			sink.asMono().subscribe(subscriber);
		}

		@Arbiter
		public void arbiter(LLL_Result r) {
			r.r1 = subscriber.onNextCalls.get() == 1 ? subscriber.receivedValues.get(0) : null;
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onErrorCalls.get() == 1 ? subscriber.error.getMessage() : null;
		}
	}
}

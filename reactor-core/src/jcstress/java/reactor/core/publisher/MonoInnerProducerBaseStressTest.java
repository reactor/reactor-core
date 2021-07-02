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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class MonoInnerProducerBaseStressTest {

	@JCStressTest
	@Outcome(id = {"1, 1, 0"}, expect = ACCEPTABLE, desc = "onNext and OnComplete delivered")
	@State
	public static class RequestAndCompleteWithValueRace {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(0L);
		final Operators.MonoInnerProducerBase<Integer> producer = new Operators.MonoInnerProducerBase<>(subscriber);

		{
			subscriber.onSubscribe(producer);
		}

		@Actor
		public void complete() {
			producer.complete(1);
		}

		@Actor
		public void request() {
			subscriber.request(1);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onNextDiscarded.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 1, 0"}, expect = ACCEPTABLE, desc = "onNext and OnComplete delivered. Cancel late")
	@Outcome(id = {"0, 0, 1"}, expect = ACCEPTABLE, desc = "Cancel delivered, complete(v) late")
	@State
	public static class CancelAndCompleteWithValueRace {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);
		final Operators.MonoInnerProducerBase<Integer> producer = new Operators.MonoInnerProducerBase<>(subscriber);

		{
			subscriber.onSubscribe(producer);
		}

		@Actor
		public void complete() {
			producer.complete(1);
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onNextDiscarded.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 1, 0"}, expect = ACCEPTABLE, desc = "onNext and OnComplete delivered. Cancel late")
	@Outcome(id = {"0, 0, 1"}, expect = ACCEPTABLE, desc = "Cancel delivered, complete(v) late")
	@State
	public static class CancelAndRequestAndCompleteWithValueRace {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(0L);
		final Operators.MonoInnerProducerBase<Integer> producer = new Operators.MonoInnerProducerBase<>(subscriber);

		{
			subscriber.onSubscribe(producer);
		}

		@Actor
		public void complete() {
			producer.complete(1);
		}

		@Actor void request() {
			producer.request(1);
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onNextDiscarded.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 1, 0"}, expect = ACCEPTABLE, desc = "onNext and OnComplete delivered. Cancel late")
	@Outcome(id = {"0, 0, 1"}, expect = ACCEPTABLE, desc = "Cancel delivered, complete(v) late")
	@State
	public static class CancelAndSetValueWithCompleteRace {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);
		final Operators.MonoInnerProducerBase<Integer> producer = new Operators.MonoInnerProducerBase<>(subscriber);

		{
			subscriber.onSubscribe(producer);
		}

		@Actor
		public void complete() {
			producer.setValue(1);
			producer.complete();
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = subscriber.onNextCalls.get();
			r.r2 = subscriber.onCompleteCalls.get();
			r.r3 = subscriber.onNextDiscarded.get();
		}
	}

}

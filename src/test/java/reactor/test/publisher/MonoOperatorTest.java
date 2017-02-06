/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.publisher;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public abstract class MonoOperatorTest<I, O>
		extends BaseOperatorTest<I, Mono<I>, O, Mono<O>> {

	public final Scenario<I, O> scenario(Function<Mono<I>, ? extends Mono<O>> scenario) {
		return Scenario.create(scenario)
		               .applyAllOptions(defaultScenario);
	}

	static public final class Scenario<I, O>
			extends OperatorScenario<I, Mono<I>, O, Mono<O>> {

		static <I, O> Scenario<I, O> create(Function<Mono<I>, ? extends Mono<O>> scenario) {
			return new Scenario<>(scenario, new Exception("scenario:"));
		}

		Scenario(Function<Mono<I>, ? extends Mono<O>> scenario, Exception stack) {
			super(scenario, stack);
		}

		@Override
		Scenario<I, O> duplicate() {
			return new Scenario<>(scenario, stack).applyAllOptions(this);
		}

		@Override
		public Scenario<I, O> shouldHitDropNextHookAfterTerminate(boolean shouldHitDropNextHookAfterTerminate) {
			super.shouldHitDropNextHookAfterTerminate(shouldHitDropNextHookAfterTerminate);
			return this;
		}

		@Override
		public Scenario<I, O> shouldHitDropErrorHookAfterTerminate(boolean shouldHitDropErrorHookAfterTerminate) {
			super.shouldHitDropErrorHookAfterTerminate(
					shouldHitDropErrorHookAfterTerminate);
			return this;
		}

		@Override
		public Scenario<I, O> shouldAssertPostTerminateState(boolean shouldAssertPostTerminateState) {
			super.shouldAssertPostTerminateState(shouldAssertPostTerminateState);
			return this;
		}

		@Override
		public Scenario<I, O> fusionMode(int fusionMode) {
			super.fusionMode(fusionMode);
			return this;
		}

		@Override
		public Scenario<I, O> fusionModeThreadBarrier(int fusionModeThreadBarrier) {
			super.fusionModeThreadBarrier(fusionModeThreadBarrier);
			return this;
		}

		@Override
		public Scenario<I, O> prefetch(int prefetch) {
			super.prefetch(prefetch);
			return this;
		}

		@Override
		public Scenario<I, O> producerEmpty() {
			super.producerEmpty();
			return this;
		}

		@Override
		public Scenario<I, O> producerNever() {
			super.producerNever();
			return this;
		}

		@Override
		public Scenario<I, O> producer(int n, IntFunction<? extends I> producer) {
			super.producer(n, producer);
			return this;
		}

		@Override
		public final Scenario<I, O> receiverEmpty() {
			super.receiverEmpty();
			return this;
		}

		@Override
		@SafeVarargs
		public final Scenario<I, O> receive(Consumer<? super O>... receivers) {
			super.receive(receivers);
			return this;
		}

		@Override
		@SafeVarargs
		public final Scenario<I, O> receiveValues(O... receivers) {
			super.receiveValues(receivers);
			return this;
		}

		@Override
		public Scenario<I, O> receiverDemand(long d) {
			super.receiverDemand(d);
			return this;
		}

		@Override
		public Scenario<I, O> description(String description) {
			super.description(description);
			return this;
		}

		@Override
		public Scenario<I, O> verifier(Consumer<StepVerifier.Step<O>> verifier) {
			super.verifier(verifier);
			return this;
		}

		@Override
		public Scenario<I, O> applyAllOptions(OperatorScenario<I, Mono<I>, O, Mono<O>> source) {
			super.applyAllOptions(source);
			return this;
		}
	}

	@Override
	protected List<Scenario<I, O>> scenarios_operatorSuccess() {
		return Collections.emptyList();
	}

	@Override
	protected List<Scenario<I, O>> scenarios_operatorError() {
		return Collections.emptyList();
	}

	@Override
	protected List<Scenario<I, O>> scenarios_errorFromUpstreamFailure() {
		return scenarios_operatorSuccess();
	}

	//assert
	protected List<Scenario<I, O>> scenarios_touchAndAssertState() {
		return scenarios_operatorSuccess();
	}
}

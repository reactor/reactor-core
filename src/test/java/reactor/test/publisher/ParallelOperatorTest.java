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

import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public abstract class ParallelOperatorTest<I, O>
		extends BaseOperatorTest<I, ParallelFlux<I>, O, ParallelFlux<O>> {
	
	public final Scenario<I, O> scenario(Function<ParallelFlux<I>, ? extends ParallelFlux<O>> scenario) {
		if (defaultEmpty) {
			return Scenario.create(scenario)
			                                .applyAllOptions(defaultScenario.duplicate()
			                                                                .receiverEmpty());
		}
		return Scenario.create(scenario)
		                                .applyAllOptions(defaultScenario);
	}

	static public final class Scenario<I, O>
			extends OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> {

		static <I, O> Scenario<I, O> create(Function<ParallelFlux<I>, ? extends ParallelFlux<O>> scenario) {
			return new Scenario<>(scenario, new Exception("scenario:"));
		}

		Scenario(Function<ParallelFlux<I>, ? extends ParallelFlux<O>> scenario, Exception stack) {
			super(scenario, stack);
		}

		@Override
		Scenario<I, O> duplicate() {
			return new Scenario<>(body, stack).applyAllOptions(this);
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
		public Scenario<I, O> producerError(RuntimeException e) {
			super.producerError(e);
			return this;
		}

		@Override
		public Scenario<I, O> droppedError(RuntimeException e) {
			super.droppedError(e);
			return this;
		}

		@Override
		public Scenario<I, O> droppedItem(I item) {
			super.droppedItem(item);
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
		public final Scenario<I, O> receive(int i, IntFunction<? extends O> receivers) {
			super.receive(i, receivers);
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
		public Scenario<I, O> applyAllOptions(OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> source) {
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

	@Override
	protected List<Scenario<I, O>> scenarios_touchAndAssertState() {
		return scenarios_operatorSuccess();
	}

	@Override
	@SuppressWarnings("unchecked")
	protected final OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> defaultScenarioOptions(
			OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> defaultOptions) {
		Scenario<I, O>
				s = new Scenario<I, O>(null, null).applyAllOptions(defaultOptions)
				                                  .producer(3,
						                                  i -> (I) (i == 0 ?
								                                  "test" :
								                                  "test" + i))
				                                  .receive(3,
						                                  i -> (O) (i == 0 ?
								                                  "test" :
								                                  "test" + i))
				                                  .droppedError(new RuntimeException("dropped"))
				                                  .droppedItem((I)"dropped");
		this.defaultScenario = s;
		return defaultScenarioOptions(s);
	}

	@SuppressWarnings("unchecked")
	protected Scenario<I, O> defaultScenarioOptions(Scenario<I, O> defaultOptions) {
		return defaultOptions;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected ParallelFlux<I> sourceScalar(OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> scenario) {
		if(scenario.producerCount() == 0){
			return (ParallelFlux<I>)Flux.empty()
			                            .parallel(4);
		}
		return (ParallelFlux<I>)Flux.just(scenario.producingMapper.apply(0))
		                            .parallel(4);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected ParallelFlux<I> sourceCallable(OperatorScenario<I, ParallelFlux<I>, O, ParallelFlux<O>> scenario) {
		if(scenario.producerCount() == 0){
			return (ParallelFlux<I>)Mono.fromRunnable(() -> {})
		        .flux()
		        .parallel(4);
		}
		return (ParallelFlux<I>) Mono.fromCallable(() -> scenario.producingMapper.apply(0))
		                             .flux()
		                             .parallel(4);
	}

	@Override
	protected ParallelFlux<I> withFluxSource(Flux<I> input) {
		return input.parallel(4);
	}

	@Override
	protected ParallelFlux<I> hide(ParallelFlux<I> input) {
		return input.hide();
	}

	@Override
	protected ParallelFlux<O> conditional(ParallelFlux<O> output) {
		return output.filter(t -> true);
	}

	@Override
	protected ParallelFlux<O> doOnSubscribe(ParallelFlux<O> output,
			Consumer<? super Subscription> doOnSubscribe) {
		return output.doOnSubscribe(doOnSubscribe);
	}
}

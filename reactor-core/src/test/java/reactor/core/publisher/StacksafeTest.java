/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Simon Baslé
 */
class StacksafeTest {

	private final int defaultDepth = Operators.stacksafeMaxOperatorDepth;

	@BeforeEach
	void setUp() {
		Operators.setStacksafeMaxOperatorDepth(1000);
	}

	@AfterEach
	void tearDown() {
		Operators.setStacksafeMaxOperatorDepth(defaultDepth);
	}

	@Test
	void stackSafeSubscriberHasMeaningfulToString() {
		FluxMap.MapSubscriber<String, Integer> subscriber = new FluxMap.MapSubscriber<>(Operators.emptySubscriber(), String::length);

		@SuppressWarnings("ConstantConditions") //intentionally null worker
		Operators.Stacksafe.StacksafeSubscriber<String> stackSafeSubscriber = new Operators.Stacksafe.StacksafeSubscriber<>(subscriber,
				null, "printsWhateverIsHere");

		assertThat(stackSafeSubscriber).hasToString("StacksafeSubscriber{printsWhateverIsHere}");
	}

	@Test
	void stacksafeGeneratesMeaningfulToString() {
		FluxMap<String, Integer> operator = new FluxMap<>(Flux.empty(), String::length);
		FluxMap.MapSubscriber<String, Integer> subscriber = new FluxMap.MapSubscriber<>(Operators.emptySubscriber(), String::length);

		Operators.Stacksafe stacksafe = new Operators.Stacksafe(1);
		CoreSubscriber<String> stackSafeSubscriber = stacksafe.protect(subscriber, operator);

		assertThat(stackSafeSubscriber).hasToString("StacksafeSubscriber{depth=1, wrapping=FluxMap}");
	}

	@Test
	void negativeMaxDepthIsNoOp() {
		CoreSubscriber<Object> subscriber = Operators.emptySubscriber();
		OptimizableOperator<Object, Object> fakeOptimizable = Mockito.mock(OptimizableOperator.class);
		Operators.Stacksafe stacksafe = new Operators.Stacksafe(-1);

		CoreSubscriber<Object> s = subscriber;
		for (int i = 0; i < 10_000; i++) {
			s = stacksafe.protect(s, fakeOptimizable);
			assertThat(s).as("no wrapping in round #" + i)
			             .isSameAs(subscriber);
		}
	}

	@Test
	void zeroMaxDepthIsNoOp() {
		CoreSubscriber<Object> subscriber = Operators.emptySubscriber();
		OptimizableOperator<Object, Object> fakeOptimizable = Mockito.mock(OptimizableOperator.class);
		Operators.Stacksafe stacksafe = new Operators.Stacksafe(0);

		CoreSubscriber<Object> s = subscriber;
		for (int i = 0; i < 10_000; i++) {
			s = stacksafe.protect(s, fakeOptimizable);
			assertThat(s).as("no wrapping in round #" + i)
			             .isSameAs(subscriber);
		}
	}

	@Test
	void largeOperatorChainWithHighMaxDepthBlowsUp() {
		//here we intentionally force a high max depth, so that the loop below doesn't trampoline
		Operators.setStacksafeMaxOperatorDepth(10_000);
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 5000; i++) {
			int currentI = i;
			tooDeep = tooDeep.map(previous -> currentI);
		}

		assertThatExceptionOfType(StackOverflowError.class)
				.isThrownBy(tooDeep::blockLast);
	}

	@Test
	void largeOperatorChainSmallerTrampoliningThresholdTrampolines() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 5000; i++) {
			int currentI = i;
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.map(previous -> currentI);
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(5000);
		assertThat(threadNames).startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("number of threads").isGreaterThan(1);
	}

	@Test
	void hugeOperatorChainSmallerTrampoliningThresholdTrampolines() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 500_000; i++) {
			int currentI = i;
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.map(previous -> currentI);
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(500_000);
		assertThat(threadNames)
				.startsWith(Thread.currentThread().getName())
				.hasSize(Runtime.getRuntime().availableProcessors() + 1);
	}

	@Test
	void hugeOperatorChainWithFlatmap() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 500_000; i++) {
			int currentI = i;
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.flatMap(previous -> Mono.just(currentI));
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(500_000);
		assertThat(threadNames)
				.startsWith(Thread.currentThread().getName())
				.hasSize(Runtime.getRuntime().availableProcessors() + 1);
	}

	@Test
	void hugeOperatorChainWithFlatMapAndSparsePublishOn() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 500_000; i++) {
			int currentI = i;
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.flatMap(previous -> Mono.just(currentI));
			if (i % 5000 == 0) {
				tooDeep = tooDeep.publishOn(Schedulers.single());
			}
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(500_000);
		assertThat(threadNames)
				.startsWith(Thread.currentThread().getName())
				.hasSize(Runtime.getRuntime().availableProcessors() + 1);
	}

	@Test
	void trampolineVanillaSubscriber() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeepNormal = Flux.just(0).hide();
		for (int i = 0; i <= 5000; i++) {
			int currentI = i;
			tooDeepNormal = tooDeepNormal
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.map(previous -> currentI);
		}

		assertThat(tooDeepNormal.blockLast(Duration.ofSeconds(5))).as("flux normal").isEqualTo(5000);
		assertThat(threadNames).as("flux normal").startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("flux normal, number of threads")
		                              .isStrictlyBetween(1, 15);
	}

	@Test
	void trampolineFuseableSubscriber() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Mono<Integer> tooDeepFuseable = Mono.just(1);
		for (int i = 0; i <= 5000; i++) {
			int currentI = i;
			tooDeepFuseable = tooDeepFuseable
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.map(previous -> currentI);
		}

		assertThat(tooDeepFuseable.block(Duration.ofSeconds(5))).as("Mono Fuseable").isEqualTo(5000);
		assertThat(threadNames).as("Mono Fuseable").startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("Mono Fuseable, number of threads")
		                              .isStrictlyBetween(1, 15);
	}

	@Test
	void trampolineConditionalSubscriber() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0).hide();

		for (int i = 0; i <= 2500; i++) { //2500: conditional doubles the number of operators
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.filter(v -> true);
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(0);
		assertThat(threadNames).startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("number of threads")
		                              .isStrictlyBetween(1, 15);
	}

	@Test
	void trampolineConditionalFuseableSubscriber() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> tooDeep = Flux.just(0);

		for (int i = 0; i <= 2500; i++) { //2500: conditional doubles the number of operators
			tooDeep = tooDeep
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()))
					.filter(v -> true);
		}

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(0);
		assertThat(threadNames).startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("number of threads")
		                              .isStrictlyBetween(1, 15);
	}

	@Test
	void smallEnoughOperatorChainNoTrampolining() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();
		Flux<Integer> shallowEnough = Flux.just(0).hide();

		for (int i = 0; i < 200; i++) {
			int currentI = i;
			shallowEnough = shallowEnough
					.map(previous -> currentI)
					.doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()));
		}

		assertThat(shallowEnough.blockLast(Duration.ofSeconds(5))).isEqualTo(199);
		assertThat(threadNames).containsOnly(Thread.currentThread().getName());
	}

	@Test
	void hugeOperatorChainWithSwitchIfEmpty() {
		Set<String> threadNames = new ConcurrentSkipListSet<>();

		final int bound = 5000;
		Flux<Integer> tooDeep = chain(0, bound, threadNames);

		assertThat(tooDeep.blockLast(Duration.ofSeconds(5))).isEqualTo(bound);
		assertThat(threadNames).startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("number of threads").isGreaterThan(1);
	}

	Flux<Integer> chain(int i, int bound, Set<String> threadNames) {
		return Flux.defer(() -> i < bound
				? Flux.<Integer>empty().switchIfEmpty(chain(i + 1, bound, threadNames))
				: Flux.just(i).hide()
		).doOnSubscribe(s -> threadNames.add(Thread.currentThread().getName()));
	}

	//TODO test with errors, debug mode, etc...
}
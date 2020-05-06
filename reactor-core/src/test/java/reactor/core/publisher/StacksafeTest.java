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

import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

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
		assertThat(threadNames).startsWith(Thread.currentThread().getName());
		assertThat(threadNames.size()).as("number of threads").isGreaterThan(1);
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

	//TODO test with errors, debug mode, etc...
}
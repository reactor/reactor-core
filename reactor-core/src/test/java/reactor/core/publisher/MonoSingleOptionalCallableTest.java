/*
 * Copyright (c) 2021-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

class MonoSingleOptionalCallableTest {

	@Test
	void testCallableFusedEmptySource() {
		Mono<Optional<Integer>> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.singleOptional();

		StepVerifier.create(mono)
		            .expectNext(Optional.empty())
		            .verifyComplete();
	}

	@Test
	void testCallableFusedSingleEmptySourceOnBlock() {
		Mono<Optional<Integer>> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.singleOptional();

		assertEquals(Optional.empty(), mono.block());
	}

	@Test
	void testCallableFusedSingleEmptySourceOnCall() throws Exception {
		Mono<Optional<Integer>> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.singleOptional();

		assertThat(mono).isInstanceOf(MonoSingleOptionalCallable.class);

		assertEquals(Optional.empty(), ((Callable<?>) mono).call());
	}

	@Test
	void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingleOptionalCallable<>(null);
		});
	}

	@Test
	void normal() {
		StepVerifier.create(new MonoSingleOptionalCallable<>(() -> 1))
		            .expectNext(Optional.of(1))
		            .verifyComplete();
	}

	@Test
	void normalBackpressured() {
		StepVerifier.create(new MonoSingleOptionalCallable<>(() -> 1), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(Optional.of(1))
		            .verifyComplete();
	}

	//scalarCallable empty/error/just are not instantiating MonoSingleOptionalCallable and are covered in MonoSingleTest
	//we still cover the case where a callable source throws

	@Test
	void failingCallable() {
		StepVerifier.create(new MonoSingleOptionalCallable<>(() -> { throw new IllegalStateException("test"); } ))
		            .verifyErrorMessage("test");
	}

	@Test
	void emptyCallable() {
		StepVerifier.create(new MonoSingleOptionalCallable<>(() -> null))
		            .expectNext(Optional.empty())
		            .verifyComplete();
	}

	@Test
	void valuedCallable() {
		@SuppressWarnings("unchecked")
		Callable<Integer> fluxCallable = (Callable<Integer>) Mono.fromCallable(() -> 1).flux();


		StepVerifier.create(new MonoSingleOptionalCallable<>(fluxCallable))
		            .expectNext(Optional.of(1))
		            .verifyComplete();
	}

	@Test
	void fusionMonoSingleOptionalCallableDoesntTriggerFusion() {
		Mono<Optional<Integer>> fusedCase = Mono
				.fromCallable(() -> 1)
				.singleOptional();

		assertThat(fusedCase)
				.as("fusedCase assembly check")
				.isInstanceOf(MonoSingleOptionalCallable.class)
				.isNotInstanceOf(Fuseable.class);

		assertThatCode(() -> fusedCase.filter(v -> true).block())
				.as("fusedCase fused")
				.doesNotThrowAnyException();
	}

	@Test
	void scanOperator(){
		MonoSingleOptionalCallable<String> test = new MonoSingleOptionalCallable<>(() -> "foo");

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}

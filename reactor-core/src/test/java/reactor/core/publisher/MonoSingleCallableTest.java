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

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

class MonoSingleCallableTest {

	@Test
	void apiFluxCallableGivesDefaultValue() {
		Flux<Integer> source = Mono.<Integer>fromSupplier(() -> null).flux();

		source.single(2)
		      .as(StepVerifier::create)
		      .expectNext(2)
		      .verifyComplete();

		assertThat(source.single(2)).isInstanceOf(MonoSingleCallable.class);
	}

	//  https://github.com/reactor/reactor-core/issues/2635
	@Test
	void ensureCallableFusedSingleFailOnEmptySource() {
		Mono<Integer> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.single();

		StepVerifier.create(mono)
		            .expectErrorMatches(err -> err instanceof NoSuchElementException)
		            .verify();
	}

	@Test
	void ensureCallableFusedSingleFailOnEmptySourceOnBlock() {
		Mono<Integer> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.single();

		assertThatThrownBy(mono::block)
				.isInstanceOf(NoSuchElementException.class);
	}

	@Test
	void ensureCallableFusedSingleFailOnEmptySourceOnCall() {
		Mono<Integer> mono = Mono
				.<Integer>fromSupplier(() -> null)
				.single();

		assertThat(mono).isInstanceOf(MonoSingleCallable.class);

		assertThatThrownBy(((Callable<?>) mono)::call)
				.isInstanceOf(NoSuchElementException.class);
	}

	@Test
	void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingleCallable<>(null);
		});
	}

	@Test
	void sourceNullWithDefaultValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingleCallable<>(null, 1);
		});
	}

	@Test
	void defaultValueNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingleCallable<>(() -> 1, null);
		});
	}

	@Test
	void normal() {
		StepVerifier.create(new MonoSingleCallable<>(() -> 1))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	void normalBackpressured() {
		StepVerifier.create(new MonoSingleCallable<>(() -> 1), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(1)
		            .verifyComplete();
	}

	//scalarCallable empty/error/just are not instantiating MonoSingleCallable and are covered in MonoSingleTest
	//we still cover the case where a callable source throws

	@Test
	void failingCallable() {
		StepVerifier.create(new MonoSingleCallable<>(() -> { throw new IllegalStateException("test"); } ))
		            .verifyErrorMessage("test");
	}

	@Test
	void failingCallableWithDefaultValue() {
		StepVerifier.create(new MonoSingleCallable<>(() -> { throw new IllegalStateException("test"); },
				"bla"))
		            .verifyErrorMessage("test");
	}

	@Test
	void emptyCallable() {
		StepVerifier.create(new MonoSingleCallable<>(() -> null))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NoSuchElementException.class)
				            .hasMessage("Source was empty")
		            );
	}

	@Test
	void emptyCallableWithDefaultValue() {
		StepVerifier.create(new MonoSingleCallable<>(() -> null, "bla"))
		            .expectNext("bla")
		            .verifyComplete();
	}

	@Test
	void emptyCallableWithDefaultValueBackpressured() {
		StepVerifier.create(new MonoSingleCallable<>(() -> null, "bla"), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext("bla")
		            .verifyComplete();
	}

	@Test
	void valuedCallable() {
		@SuppressWarnings("unchecked")
		Callable<Integer> fluxCallable = (Callable<Integer>) Mono.fromCallable(() -> 1).flux();


		StepVerifier.create(new MonoSingleCallable<>(fluxCallable))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	void valuedCallableWithDefaultValue() {
		@SuppressWarnings("unchecked")
		Callable<Integer> fluxCallable = (Callable<Integer>) Mono.fromCallable(() -> 1).flux();

		StepVerifier.create(new MonoSingleCallable<>(fluxCallable, 2))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	void defaultValueReturnedInBlock() {
		Callable<Integer> source = () -> null;
		Mono<Integer> single = new MonoSingleCallable<>(source, 2);

		assertThat(single.block()).isEqualTo(2);
	}

	@Test
	void defaultValueReturnedInCall() throws Exception {
		Callable<Integer> source = () -> null;
		Callable<Integer> single = new MonoSingleCallable<>(source, 2);

		assertThat(single.call()).isEqualTo(2);
	}

	// see https://github.com/reactor/reactor-core/issues/2663
	@Test
	void fusionMonoSingleCallableDoesntTriggerFusion() {
		Mono<Integer> fusedCase = Mono
				.fromCallable(() -> 1)
				.single();

		assertThat(fusedCase)
				.as("fusedCase assembly check")
				.isInstanceOf(MonoSingleCallable.class)
				.isNotInstanceOf(Fuseable.class);

		assertThatCode(() -> fusedCase.filter(v -> true).block())
				.as("fusedCase fused")
				.doesNotThrowAnyException();
	}

	@Test
	void scanOperator(){
		MonoSingleCallable<String> test = new MonoSingleCallable<>(() -> "foo");

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}

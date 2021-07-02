/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class MonoSingleMonoTest {

	@Test
	public void callableEmpty() {
		StepVerifier.create(Mono.empty().single())
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NoSuchElementException.class)
				            .hasMessage("Source was a (constant) empty"));
	}

	@Test
	public void callableValued() {
		StepVerifier.create(Mono.just("foo").single())
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Mono.empty().hide().single())
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NoSuchElementException.class)
				            .hasMessage("Source was empty"));
	}

	@Test
	public void normalValued() {
		StepVerifier.create(Mono.just("foo").hide().single())
		            .expectNext("foo")
		            .verifyComplete();
	}

	// see https://github.com/reactor/reactor-core/issues/2663
	@Test
	void fusionMonoSingleMonoDoesntTriggerFusion() {
		Mono<Integer> fusedCase = Mono
				.just(1)
				.map(Function.identity())
				.single();

		assertThat(fusedCase)
				.as("fusedCase assembly check")
				.isInstanceOf(MonoSingleMono.class)
				.isNotInstanceOf(Fuseable.class);

		assertThatCode(() -> fusedCase.filter(v -> true).block())
				.as("fusedCase fused")
				.doesNotThrowAnyException();
	}

	@Test
	public void scanOperator(){
	    MonoSingleMono<String> test = new MonoSingleMono<>(Mono.just("foo"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

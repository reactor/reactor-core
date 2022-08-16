/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * For tests that actually assert things when context-propagation is available, see
 * {@code withMicrometer} test set.
 * @author Simon Baslé
 */
class ContextPropagationNotThereSmokeTest {

	@Test
	void contextPropagationIsNotAvailable() {
		assertThat(ContextPropagation.isContextPropagationAvailable()).isFalse();
	}

	@Test
	void contextCaptureIsNoOp() {
		assertThat(ContextPropagation.contextCapture()).as("without predicate").isSameAs(ContextPropagation.NO_OP);
		assertThat(ContextPropagation.contextCapture(v -> true)).as("with predicate").isSameAs(ContextPropagation.NO_OP);
	}

	@Test
	void contextCaptureFluxApiIsNoOp() {
		Flux<Integer> source = Flux.empty();
		assertThat(source.contextCapture()).isSameAs(source);
	}

	@Test
	void contextCaptureMonoApiIsNoOp() {
		Mono<Integer> source = Mono.empty();
		assertThat(source.contextCapture()).isSameAs(source);
	}

}

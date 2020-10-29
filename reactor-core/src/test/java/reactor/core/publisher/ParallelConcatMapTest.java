/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelConcatMapTest {

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelConcatMap<Integer, Integer> test = new ParallelConcatMap<>(source,
				i -> Flux.range(1, i), Queues.small(), 123,
				FluxConcatMap.ErrorMode.IMMEDIATE);

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void scanOperator() throws Exception {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelConcatMap<Integer, Integer> test = new ParallelConcatMap<>(source,
				i -> Flux.range(1, i), Queues.small(), 123,
				FluxConcatMap.ErrorMode.IMMEDIATE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(123)
				.isNotEqualTo(source.getPrefetch());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();
	}

	@Test
	public void scanOperatorErrorModeBoundary() throws Exception {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelConcatMap<Integer, Integer> test = new ParallelConcatMap<>(source,
				i -> Flux.range(1, i), Queues.small(), 123,
				FluxConcatMap.ErrorMode.BOUNDARY);

		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
	}

	@Test
	public void scanOperatorErrorModeEnd() throws Exception {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelConcatMap<Integer, Integer> test = new ParallelConcatMap<>(source,
				i -> Flux.range(1, i), Queues.small(), 123,
				FluxConcatMap.ErrorMode.END);

		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatDelayErrorWithFluxError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Flux.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .parallel()
				    .concatMapDelayError(f -> f, true, 32)
				    .sequential())
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatDelayErrorWithMonoError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Mono.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .parallel()
				    .concatMapDelayError(f -> f, true, 32)
				    .sequential())
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

}

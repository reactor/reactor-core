/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelFluxOnAssemblyTest {

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source);

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void scanUnsafe() {
		FluxCallableOnAssembly<?> test = new FluxCallableOnAssembly<>(Flux.empty());

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
	}

	@Test
	public void stepNameAndToString() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source, "foo");

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.isEqualTo("reactor.core.publisher.ParallelFluxOnAssemblyTest.stepNameAndToString(ParallelFluxOnAssemblyTest.java:46)");
	}

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(256)
				.isEqualTo(source.getPrefetch());

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
	}

}
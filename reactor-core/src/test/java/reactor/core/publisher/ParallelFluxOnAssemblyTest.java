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
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

public class ParallelFluxOnAssemblyTest {

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source, stacktrace);

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void stepNameAndToString() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		AssemblySnapshot stacktrace = new AssemblySnapshot("foo", Traces.callSiteSupplierFactory.get());
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source, stacktrace);

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.startsWith("reactor.core.publisher.ParallelFluxOnAssemblyTest.stepNameAndToString(ParallelFluxOnAssemblyTest.java:");
	}

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
		ParallelFluxOnAssembly<Integer> test = new ParallelFluxOnAssembly<>(source, stacktrace);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(256)
				.isEqualTo(source.getPrefetch());

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
		assertThat(test.scan(RUN_STYLE)).isSameAs(SYNC);
	}

}

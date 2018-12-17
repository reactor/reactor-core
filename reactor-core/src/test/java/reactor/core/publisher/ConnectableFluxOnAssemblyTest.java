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

package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectableFluxOnAssemblyTest {

	@Test
	public void scanMain() throws Exception {
		ConnectableFlux<String> source = Flux.just("foo").publish();
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Tracer.callSiteSupplierFactory.get());
		ConnectableFluxOnAssembly<String> test = new ConnectableFluxOnAssembly<>(source, stacktrace);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
	}

	@Test
	public void scanOperator() {
		ConnectableFlux<String> source = Flux.just("foo").publish();
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Tracer.callSiteSupplierFactory.get());
		ConnectableFluxOnAssembly<String> test = new ConnectableFluxOnAssembly<>(source, stacktrace);

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(source);
	}

	@Test
	public void stepNameAndToString() {
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Tracer.callSiteSupplierFactory.get());
		ConnectableFluxOnAssembly<?> test = new ConnectableFluxOnAssembly<>(Flux.empty().publish(), stacktrace);

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.startsWith("reactor.core.publisher.ConnectableFluxOnAssemblyTest.stepNameAndToString(ConnectableFluxOnAssemblyTest.java:");
	}

}
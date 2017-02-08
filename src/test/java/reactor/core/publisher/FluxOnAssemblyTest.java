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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshotException;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxOnAssemblyTest {

	@Test
	public void stacktraceHeaderTraceEmpty() {
		StringBuilder sb = new StringBuilder();
		AssemblySnapshotException e = new AssemblySnapshotException(null, null);

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, e);

		assertThat(sb.toString())
				.isEqualTo("\nAssembly trace from producer [java.lang.String] :\n");
	}

	@Test
	public void stacktraceHeaderTraceDescription() {
		StringBuilder sb = new StringBuilder();
		AssemblySnapshotException e = new AssemblySnapshotException("This is readable", null);

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, e);

		assertThat(sb.toString())
				.startsWith("\nAssembly trace from producer [java.lang.String]")
				.endsWith(", described as [This is readable] :\n");
	}

	@Test
	public void stacktraceHeaderTraceCorrelation() {
		StringBuilder sb = new StringBuilder();
		AssemblySnapshotException e = new AssemblySnapshotException(null, "1234");

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, e);

		assertThat(sb.toString())
				.startsWith("\nAssembly trace from producer [java.lang.String]")
				.endsWith(", correlationId [1234] :\n");
	}

	@Test
	public void stacktraceHeaderTraceDescriptionAndCorrelation() {
		StringBuilder sb = new StringBuilder();
		AssemblySnapshotException e = new AssemblySnapshotException("This is readable", "1234");

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, e);

		assertThat(sb.toString())
				.startsWith("\nAssembly trace from producer [java.lang.String]")
				.endsWith(", described as [This is readable], correlationId [1234] :\n");
	}

	@Test
	public void checkpointEmpty() {
		StringWriter sw = new StringWriter();

		Flux.range(1, 10)
		    .map(i -> null)
		    .checkpoint()
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxMapFuseable] :");
	}

	@Test
	public void checkpointDescription() {
		StringWriter sw = new StringWriter();

		Flux.range(1, 10)
		    .map(i -> null)
		    .checkpoint("foo")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxMapFuseable], described as [foo] :");
	}

	@Test
	public void checkpointCorrelation() {
		StringWriter sw = new StringWriter();

		Flux.range(1, 10)
		    .map(i -> null)
		    .checkpoint(null, "1234")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxMapFuseable], correlationId [1234] :");
	}

	@Test
	public void checkpointDescriptionCorrelation() {
		StringWriter sw = new StringWriter();

		Flux.range(1, 10)
		    .map(i -> null)
		    .checkpoint("foo", "1234")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxMapFuseable], described as [foo], correlationId [1234] :");
	}

	@Test
	public void monoCheckpointEmpty() {
		StringWriter sw = new StringWriter();

		Mono.just(1)
		    .map(i -> null)
		    .checkpoint()
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoMapFuseable] :");
	}

	@Test
	public void monoCheckpointDescription() {
		StringWriter sw = new StringWriter();

		Mono.just(1)
		    .map(i -> null)
		    .checkpoint("foo")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoMapFuseable], described as [foo] :");
	}

	@Test
	public void monoCheckpointCorrelation() {
		StringWriter sw = new StringWriter();

		Mono.just(1)
		    .map(i -> null)
		    .checkpoint(null, "1234")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoMapFuseable], correlationId [1234] :");
	}

	@Test
	public void monoCheckpointDescriptionCorrelation() {
		StringWriter sw = new StringWriter();

		Mono.just(1)
		    .map(i -> null)
		    .checkpoint("foo", "1234")
		    .subscribe(System.out::println, t -> t.printStackTrace(new PrintWriter(sw)));

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoMapFuseable], described as [foo], correlationId [1234] :");
	}

}
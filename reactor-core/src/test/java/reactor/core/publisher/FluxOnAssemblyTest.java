/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class FluxOnAssemblyTest {

	@After
	public void tearDown() {
		Hooks.resetOnOperatorDebug();
	}

	@Test
	public void stacktraceHeaderTraceEmpty() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, null);

		assertThat(sb.toString())
				.isEqualTo("\nAssembly trace from producer [java.lang.String] :\n");
	}

	@Test
	public void stacktraceHeaderTraceDescriptionNull() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, null);

		assertThat(sb.toString())
				.isEqualTo("\nAssembly trace from producer [java.lang.String] :\n");
	}

	@Test
	public void stacktraceHeaderTraceDescription() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, "1234");

		assertThat(sb.toString())
				.startsWith("\nAssembly trace from producer [java.lang.String]")
				.endsWith(", described as [1234] :\n");
	}

	@Test
	public void checkpointEmpty() {
		StringWriter sw = new StringWriter();

		Flux<Integer> tested = Flux.range(1, 10)
		                           .map(i -> i < 3 ? i : null)
		                           .filter(i -> i % 2 == 0)
		                           .checkpoint()
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));
		StepVerifier.create(tested)
		            .expectNext(2)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxFilterFuseable] :");
	}

	@Test
	public void checkpointEmptyAndDebug() {
		StringWriter sw = new StringWriter();

		Hooks.onOperatorDebug();

		try {
			Flux<Integer> tested = Flux.range(1, 10)
			                           .map(i -> i < 3 ? i : null)
			                           .filter(i -> i % 2 == 0)
			                           .checkpoint()
			                           .doOnError(t -> t.printStackTrace(new PrintWriter(
					                           sw)));
			StepVerifier.create(tested)
			            .expectNext(2)
			            .verifyError();

			String debugStack = sw.toString();

			assertThat(debugStack).contains(
					"Assembly trace from producer [reactor.core.publisher.FluxMapFuseable] :");
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
	}

	@Test
	public void checkpointDescriptionAndForceStack() {
		StringWriter sw = new StringWriter();
		Flux<Integer> tested = Flux.range(1, 10)
		                           .map(i -> i < 3 ? i : null)
		                           .filter(i -> i % 2 == 0)
		                           .checkpoint("foo", true)
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .expectNext(2)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.FluxFilterFuseable], described as [foo] :");
	}

	@Test
	public void checkpointWithDescriptionIsLight() {
		StringWriter sw = new StringWriter();
		Flux<Integer> tested = Flux.range(1, 10)
		                           .map(i -> i < 3 ? i : null)
		                           .filter(i -> i % 2 == 0)
		                           .checkpoint("foo")
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .expectNext(2)
		            .verifyError();

		String debugStack = sw.toString();

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines.next())
				.as("first backtrace line")
				.endsWith("checkpoint ⇢ foo");
	}

	@Test
	public void monoCheckpointEmpty() {
		StringWriter sw = new StringWriter();
		Mono<Object> tested = Mono.just(1)
		                          .map(i -> null)
		                          .filter(Objects::nonNull)
		                          .checkpoint()
		                          .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoFilterFuseable] :");
	}

	@Test
	public void monoCheckpointDescriptionAndForceStack() {
		StringWriter sw = new StringWriter();
		Mono<Object> tested = Mono.just(1)
		                          .map(i -> null)
		                          .filter(Objects::nonNull)
		                          .checkpoint("foo", true)
		                          .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.MonoFilterFuseable], described as [foo] :");
	}

	@Test
	public void monoCheckpointWithDescriptionIsLight() {
		StringWriter sw = new StringWriter();
		Mono<Object> tested = Mono.just(1)
		                          .map(i -> null)
		                          .filter(Objects::nonNull)
		                          .checkpoint("foo")
		                          .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines.next())
				.as("first backtrace line")
				.endsWith("checkpoint ⇢ foo");
	}

	@Test
	public void parallelFluxCheckpointEmpty() {
		StringWriter sw = new StringWriter();
		Flux<Integer> tested = Flux.range(1, 10)
		                         .parallel(2)
		                         .transformGroups(g -> g.map(i -> (Integer) null))
		                         .checkpoint()
		                         .sequential()
		                         .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.ParallelSource] :");
	}

	@Test
	public void parallelFluxCheckpointDescriptionAndForceStack() {
		StringWriter sw = new StringWriter();
		int baseline = getBaseline();
		Flux<Integer> tested = Flux.range(1, 10)
		                           .parallel(2)
		                           .transformGroups(g -> g.map(i -> (Integer) null))
		                           .checkpoint("descriptionCorrelation1234", true)
		                           .sequential()
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		assertThat(debugStack).contains("Assembly trace from producer [reactor.core.publisher.ParallelSource], described as [descriptionCorrelation1234] :\n"
				+ "\treactor.core.publisher.ParallelFlux.checkpoint(ParallelFlux.java:244)\n"
				+ "\treactor.core.publisher.FluxOnAssemblyTest.parallelFluxCheckpointDescriptionAndForceStack(FluxOnAssemblyTest.java:" + (baseline + 4) + ")\n");

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines)
				.startsWith(
						"|_ ParallelFlux.checkpoint ⇢ at reactor.core.publisher.FluxOnAssemblyTest.parallelFluxCheckpointDescriptionAndForceStack(FluxOnAssemblyTest.java:" + (baseline + 4) + ")",
						"Stack trace:"
				);
	}

	@Test
	public void parallelFluxCheckpointDescriptionIsLight() {
		StringWriter sw = new StringWriter();
		Flux<Integer> tested = Flux.range(1, 10)
		                           .parallel(2)
		                           .transformGroups(g -> g.map(i -> (Integer) null))
		                           .checkpoint("light checkpoint identifier")
		                           .sequential()
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines.next())
				.as("first backtrace line")
				.endsWith("checkpoint ⇢ light checkpoint identifier");
	}

	@Test
	public void onAssemblyDescription() {
		String fluxOnAssemblyStr = Flux.just(1).checkpoint("onAssemblyDescription").toString();
		String expectedDescription = "checkpoint(\"onAssemblyDescription\")";
		assertTrue("Description not included: " + fluxOnAssemblyStr, fluxOnAssemblyStr.contains(expectedDescription));

		String parallelFluxOnAssemblyStr = Flux.range(1, 10).parallel(2).checkpoint("onAssemblyDescription").toString();
		assertTrue("Description not included: " + parallelFluxOnAssemblyStr, parallelFluxOnAssemblyStr.contains(expectedDescription));
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		AssemblySnapshot snapshot = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
		FluxOnAssembly.OnAssemblySubscriber<Integer> test =
        		new FluxOnAssembly.OnAssemblySubscriber<>(actual, snapshot, Flux.just(1));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
    }

	@Test
	public void scanOperator() {
		Flux<?> source = Flux.empty();
		FluxOnAssembly<?> test = new FluxOnAssembly<>(source, new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(source);
	}

	@Test
	public void stepNameAndToString() {
		int baseline = getBaseline();
		FluxOnAssembly<?> test = new FluxOnAssembly<>(Flux.empty(), new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.isEqualTo("reactor.core.publisher.FluxOnAssemblyTest.stepNameAndToString(FluxOnAssemblyTest.java:" + (baseline + 1) + ")");
	}

	@Test
	public void stackAndLightCheckpoint() {
		Hooks.onOperatorDebug();
		StringWriter sw = new StringWriter();
		Mono<Integer> tested = Flux.just(1, 2)
		                           .single()
		                           .checkpoint("single")
		                           .doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines.next())
				.as("first backtrace line")
				.contains("Flux.single ⇢ at reactor.core.publisher.FluxOnAssemblyTest.stackAndLightCheckpoint(FluxOnAssemblyTest.java:");

		assertThat(lines.next())
				.as("second backtrace line")
				.endsWith("checkpoint ⇢ single");
	}

	@Test
	public void checkpointedPublisher() {
		StringWriter sw = new StringWriter();
		Publisher<?> tested = Flux
				.just(1, 2)
				.doOnNext(__ -> {
					throw new RuntimeException("Boom");
				})
				.checkpoint("after1")
				.checkpoint("after2")
				.doOnError(t -> t.printStackTrace(new PrintWriter(sw)));

		StepVerifier.create(tested)
		            .verifyError();

		String debugStack = sw.toString();

		Iterator<String> lines = seekToBacktrace(debugStack);

		assertThat(lines.next())
				.as("first backtrace line")
				.endsWith("checkpoint ⇢ after1");

		assertThat(lines.next())
				.as("second backtrace line")
				.endsWith("checkpoint ⇢ after2");
	}

	private Iterator<String> seekToBacktrace(String debugStack) {
		Iterator<String> lines = seekToSupressedAssembly(debugStack);
		while (lines.hasNext()) {
			String line = lines.next();
			if (line.equals("Error has been observed at the following site(s):")) {
				return lines;
			}
		}
		throw new IllegalStateException("Not found!");
	}

	private Iterator<String> seekToSupressedAssembly(String debugStack) {
		Iterator<String> lines = Stream.of(debugStack.split("\n"))
		                               .map(String::trim)
		                               .iterator();
		while (lines.hasNext()) {
			String line = lines.next();
			if (line.endsWith("Suppressed: reactor.core.publisher.FluxOnAssembly$OnAssemblyException:")) {
				return lines;
			}
		}
		throw new IllegalStateException("Not found!");
	}

	private static int getBaseline() {
		return new Exception().getStackTrace()[1].getLineNumber();
	}
}
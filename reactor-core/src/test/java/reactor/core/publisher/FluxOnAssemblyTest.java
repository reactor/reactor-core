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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.assertThat;

class FluxOnAssemblyTest {

	@Test
	void dontAddCheckpointTwiceToBacktraceInCaseExceptionIsReusedInRetry() {
		final RuntimeException reusedException = new RuntimeException("reused in one retry cycle");

		StepVerifier.create(Flux.error(reusedException)
				.checkpoint("checkpointId")
				.retryWhen(Retry.max(3))
			)
			.expectErrorSatisfies(t -> {
				assertThat(t)
					.matches(Exceptions::isRetryExhausted, "isRetryExhausted")
					.hasMessage("Retries exhausted: 3/3")
					.hasCauseReference(reusedException);
				assertThat(Arrays.asList(t.getCause().getSuppressed()))
					.as("backtrace as suppressed on the cause")
					.hasSize(1)
					.first(InstanceOfAssertFactories.THROWABLE)
					.hasMessage("\n" +
						"Error has been observed at the following site(s):\n" +
						"\t|_ checkpoint ⇢ checkpointId\n" +
						"Stack trace:");
			})
			.verify(Duration.ofSeconds(10));
	}

	@Test
	void dontAddCheckpointTwiceToBacktraceInCaseExceptionIsSharedAndMultipleRetry() {
		Throwable reusedException = new RuntimeException("reused and shared between two retries");
		StepVerifier.create(Flux.error(reusedException)
				.checkpoint("checkpointId1")
				.retryWhen(Retry.max(3))
				.onErrorResume(e -> Flux.error(reusedException)
					.checkpoint("checkpointId2")
					.retryWhen(Retry.max(2))
				)
			)
			.expectErrorSatisfies(t -> {
				assertThat(t)
					.matches(Exceptions::isRetryExhausted, "isRetryExhausted")
					.hasMessage("Retries exhausted: 2/2")
					.hasCauseReference(reusedException);
				assertThat(Arrays.asList(t.getCause().getSuppressed()))
					.as("backtrace as suppressed on the cause")
					.hasSize(1)
					.first(InstanceOfAssertFactories.THROWABLE)
					.hasMessage("\n" +
						"Error has been observed at the following site(s):\n" +
						"\t|_ checkpoint ⇢ checkpointId1\n" +
						"\t|_ checkpoint ⇢ checkpointId2\n" +
						"Stack trace:");
			})
			.verify(Duration.ofSeconds(10));
	}

	@Test
	void stacktraceHeaderTraceEmpty() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, null);

		assertThat(sb.toString())
				.isEqualTo("\nAssembly trace from producer [java.lang.String] :\n");
	}

	@Test
	void stacktraceHeaderTraceDescriptionNull() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, null);

		assertThat(sb.toString())
				.isEqualTo("\nAssembly trace from producer [java.lang.String] :\n");
	}

	@Test
	void stacktraceHeaderTraceDescription() {
		StringBuilder sb = new StringBuilder();

		FluxOnAssembly.fillStacktraceHeader(sb, String.class, "1234");

		assertThat(sb.toString())
				.startsWith("\nAssembly trace from producer [java.lang.String]")
				.endsWith(", described as [1234] :\n");
	}

	@Test
	void checkpointEmpty() {
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
	void checkpointEmptyAndDebug() {
		StringWriter sw = new StringWriter();

		Hooks.onOperatorDebug();

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

	@Test
	void checkpointDescriptionAndForceStack() {
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
	void checkpointWithDescriptionIsLight() {
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
	void monoCheckpointEmpty() {
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
	void monoCheckpointDescriptionAndForceStack() {
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
	void monoCheckpointWithDescriptionIsLight() {
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
	void parallelFluxCheckpointEmpty() {
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
	void parallelFluxCheckpointDescriptionAndForceStack() {
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
				.toIterable()
				.startsWith(
						"|_ ParallelFlux.checkpoint ⇢ at reactor.core.publisher.FluxOnAssemblyTest.parallelFluxCheckpointDescriptionAndForceStack(FluxOnAssemblyTest.java:" + (baseline + 4) + ")",
						"Stack trace:"
				);
	}

	@Test
	void parallelFluxCheckpointDescriptionIsLight() {
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
	void onAssemblyDescription() {
		String fluxOnAssemblyStr = Flux.just(1).checkpoint("onAssemblyDescription").toString();
		String expectedDescription = "checkpoint(\"onAssemblyDescription\")";
		assertThat(fluxOnAssemblyStr).contains(expectedDescription);
		String parallelFluxOnAssemblyStr = Flux.range(1, 10).parallel(2).checkpoint("onAssemblyDescription").toString();
		assertThat(parallelFluxOnAssemblyStr).contains(expectedDescription);
	}

	@Test
	void scanSubscriber() {
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
	void scanOperator() {
		Flux<?> source = Flux.empty();
		FluxOnAssembly<?> test = new FluxOnAssembly<>(source, new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA").isTrue();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(source);
	}

	@Test
	void stepNameAndToString() {
		int baseline = getBaseline();
		FluxOnAssembly<?> test = new FluxOnAssembly<>(Flux.empty(), new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.isEqualTo("reactor.core.publisher.FluxOnAssemblyTest.stepNameAndToString(FluxOnAssemblyTest.java:" + (baseline + 1) + ")");
	}

	@Test
	void stackAndLightCheckpoint() {
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
	void checkpointedPublisher() {
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

///*
// * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *       https://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package reactor.core.publisher;
//
//import java.util.Iterator;
//import java.util.regex.Pattern;
//import java.util.stream.Stream;
//
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import reactor.core.publisher.FluxDebugStatsReportTest.StatsTestSupport;
//import reactor.util.Loggers;
//import reactor.util.stats.Stats;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.fail;
//
//public class MonoDebugStatsReportTest {
//
//	@BeforeAll
//	public static void initLoggerOnce() {
//		Hooks.enableStatsRecording();
//	}
//
//	@AfterAll
//	public static void resetLoggerOnce() {
//		Loggers.resetLoggerFactory();
//		Hooks.resetOnOperatorDebug();
//		Hooks.disableStatsRecording();
//	}
//
//	@BeforeEach
//	public void resetOut() {
//		StatsTestSupport.overriddenOut.reset();
//		Hooks.onOperatorDebug();
//	}
//
//	@Test
//	public void shouldAddStatsInfo() {
//		int baseline = getBaseline();
//		Mono.just(1)
//		    .map(i -> i < 3 ? i : null)
//		    .as(this::methodReturningMono)
//		    .block();
//
//		String debugStack = StatsTestSupport.overriddenOut.toString();
//
//		Iterator<String> lines = Stream.of(debugStack.split("\n"))
//		                               .iterator();
//
//		while (lines.hasNext()) {
//			String line = lines.next();
//			if (line.endsWith("The following stats are collected:")) {
//				break;
//			}
//		}
//
//		assertThat(lines.next())
//			.as("first backtrace line")
//			.isEqualTo(
//				"  \t|  Mono.just ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfo(MonoStatsReportTest.java:" + (baseline + 1) + ")");
//		assertThat(lines.next())
//			.as("first stats-trace line")
//			.matches(Pattern.compile(
//				"  \t\\|_           ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: onNext; State: completed\\)"
//			));
//
//		assertThat(lines.next())
//			.as("second backtrace line")
//			.endsWith(
//				"  \t|  Mono.map  ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfo(MonoStatsReportTest.java:" + (baseline + 2) + ")");
//		assertThat(lines.next())
//			.as("second stats-trace line")
//			.matches(Pattern.compile(
//				"  \t\\|_           ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: onNext; State: completed\\)"
//			));
//
//		assertThat(lines.hasNext())
//			.as("should have no extra lines")
//			.isFalse();
//	}
//
//	@Test
//	public void shouldAddStatsInfoConcatMap() {
//		int baseline = getBaseline();
//		Mono.just(1)
//		    .flatMap(i -> Mono.just(i)
//		                      .map(k -> k < 3 ? k : null))
//		    .as(this::methodReturningMono)
//		    .block();
//
//		String debugStack = StatsTestSupport.overriddenOut.toString();
//
//		Iterator<String> lines = Stream.of(debugStack.split("\n"))
//		                               .iterator();
//
//		while (lines.hasNext()) {
//			String line = lines.next();
//			if (line.endsWith("The following stats are collected:")) {
//				break;
//			}
//		}
//
//		assertThat(lines.next())
//				.as("first backtrace line")
//				.isEqualTo(
//						"  \t|  Mono.just    ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoConcatMap(MonoStatsReportTest.java:" + (baseline + 1) + ")");
//		assertThat(lines.next())
//				.as("first stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|_              ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: onNext; State: completed\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second backtrace line")
//				.endsWith(
//						"  \t|  Mono.flatMap ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoConcatMap(MonoStatsReportTest.java:" + (baseline + 2) + ")");
//		assertThat(lines.next())
//				.as("second stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|_              ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: onNext; State: completed\\)"
//				));
//
//		assertThat(lines.hasNext())
//				.as("should have no extra lines")
//				.isFalse();
//	}
//
//	@Test
//	public void shouldAddStatsInfoWhenError() {
//		int baseline = 0;
//		try {
//			baseline = getBaseline();
//			Mono.just(5)
//			    .map(i -> i < 3 ? i : null)
//			    .as(this::methodReturningMono)
//			    .block();
//			fail("Should not complete successfully");
//		} catch (Exception e) {
//			// ignored
//		}
//
//		String debugStack = StatsTestSupport.overriddenOut.toString();
//
//		Iterator<String> lines = Stream.of(debugStack.split("\n"))
//		                               .iterator();
//
//		while (lines.hasNext()) {
//			String line = lines.next();
//			if (line.endsWith("The following stats are collected:")) {
//				break;
//			}
//		}
//
//		assertThat(lines.next())
//				.as("first backtrace line")
//				.isEqualTo(
//						"  \t|  Mono.just ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoWhenError(MonoStatsReportTest.java:" + (baseline + 1) + ")");
//		assertThat(lines.next())
//				.as("first stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|_           ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal:  onNext; State: canceled\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second backtrace line")
//				.isEqualTo(
//						"  \t|  Mono.map  ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoWhenError(MonoStatsReportTest.java:" + (baseline + 2) + ")");
//		assertThat(lines.next())
//				.as("second stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|_           ↳ Stats\\(Requested: ∞; Produced: 0; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: request; State:  errored\\)"
//				));
//
//		assertThat(lines.hasNext())
//				.as("should have no extra lines")
//				.isFalse();
//	}
//
//	@Test
//	public void shouldAddStatsInfoWhenConcatMapError() {
//		int baseline = 0;
//		try {
//			baseline = getBaseline();
//			Mono.just(5)
//			    .flatMap(v -> Mono.just(v)
//			                      .map(i -> i < 3 ? i : null))
//			    .as(this::methodReturningMono)
//			    .block();
//			fail("Should not complete successfully");
//		} catch (Exception e) {
//			// ignored
//		}
//
//		String debugStack = StatsTestSupport.overriddenOut.toString();
//
//		Iterator<String> lines = Stream.of(debugStack.split("\n"))
//		                               .iterator();
//
//		while (lines.hasNext()) {
//			String line = lines.next();
//			if (line.endsWith("The following stats are collected:")) {
//				break;
//			}
//		}
//
//		assertThat(lines.next())
//				.as("first backtrace line")
//				.isEqualTo(
//						"  \t|  Mono.just    ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoWhenConcatMapError(MonoStatsReportTest.java:" + (baseline + 1) + ")");
//		assertThat(lines.next())
//				.as("first stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|_              ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal:  onNext; State: completed\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second backtrace line")
//				.isEqualTo(
//						"  \t|  Mono.flatMap ⇢ at reactor.core.publisher.MonoStatsReportTest.shouldAddStatsInfoWhenConcatMapError(MonoStatsReportTest.java:" + (baseline + 2) + ")");
//		assertThat(lines.next())
//				.as("second stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|               ↳ Stats\\(Requested: ∞; Produced: 0; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: request; State:   errored\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second blank separator with inner line")
//				.isEqualTo(
//						"  \t|");
//
//		assertThat(lines.next())
//				.as("first inner backtrace line")
//				.matches(
//						"  \t\\|             0 \\|  Mono.just ⇢ at reactor.core.publisher.MonoStatsReportTest.lambda\\$shouldAddStatsInfoWhenConcatMapError\\$[0-9]\\(MonoStatsReportTest.java:" + (baseline + 2) + "\\)");
//		assertThat(lines.next())
//				.as("first inner stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|               \\|_           ↳ Stats\\(Requested: ∞; Produced: 1; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal:  onNext; State: canceled\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second inner backtrace line")
//				.matches(
//						"  \t\\|               \\|  Mono.map  ⇢ at reactor.core.publisher.MonoStatsReportTest.lambda\\$shouldAddStatsInfoWhenConcatMapError\\$[0-9]\\(MonoStatsReportTest.java:" + (baseline + 3) + "\\)");;
//		assertThat(lines.next())
//				.as("second inner stats-trace line")
//				.matches(Pattern.compile(
//						"  \t\\|               \\|_           ↳ Stats\\(Requested: ∞; Produced: 0; OnNext ~Time: [\\s0-9]+[nμm\\s]s; Last Signal: request; State:  errored\\)"
//				));
//
//		assertThat(lines.next())
//				.as("second blank separator after inner line")
//				.isEqualTo(
//						"  \t|_");
//
//		assertThat(lines.hasNext())
//				.as("should have no extra lines")
//				.isFalse();
//	}
//
//	static final int methodReturningMonoBaseline = getBaseline();
//	private Mono<Integer> methodReturningMono(Mono<Integer> mono) {
//		return mono;
//	}
//
//	private static int getBaseline() {
//		return new Exception().getStackTrace()[1].getLineNumber();
//	}
//}

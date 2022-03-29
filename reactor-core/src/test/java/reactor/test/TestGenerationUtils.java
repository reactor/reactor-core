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

package reactor.test;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.function.ThrowingConsumer;

import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
public class TestGenerationUtils {

	public static <R, T> Stream<DynamicTest> generateScheduledWithContextInScopeTests(
		String operatorName, Flux<R> source,
		Function<Flux<R>, Flux<T>> operatorUnderTest,
		ThrowingConsumer<ScheduledWithContextInScopeFluxTest<T, Void>> dynamicTestGenerator
	) {
		return generateScheduledWithContextInScopeTests(operatorName, () -> null, threadLocalCompanion -> source,
			(src, threadLocalCompanion) -> operatorUnderTest.apply(src), dynamicTestGenerator);
	}

	public static <R, T, RES> Stream<DynamicTest> generateScheduledWithContextInScopeTests(
		String operatorName,
		Supplier<RES> resourceSupplier,
		Function<ThreadLocalCompanion<RES>, Flux<R>> sourceGenerator,
		BiFunction<Flux<R>, ThreadLocalCompanion<RES>, Flux<T>> operatorUnderTest,
		ThrowingConsumer<ScheduledWithContextInScopeFluxTest<T, RES>> dynamicTestGenerator
	) {
		String testName = operatorName + "ScheduledWithContextInScope";

		ThreadLocalCompanion<RES> mainThreadLocalCompanion = new ThreadLocalCompanion<>(resourceSupplier.get());
		Flux<R> mainSource = sourceGenerator.apply(mainThreadLocalCompanion);
		Flux<T> mainSourceAndOperator = operatorUnderTest.apply(mainSource, mainThreadLocalCompanion);

		if (mainSource instanceof Fuseable && mainSourceAndOperator instanceof Fuseable) {
			//both the source and the operator decorating the source are Fuseable. Create a hidden version.
			ThreadLocalCompanion<RES> hiddenThreadLocalCompanion = new ThreadLocalCompanion<>(resourceSupplier.get());
			Flux<R> hiddenSource = sourceGenerator.apply(hiddenThreadLocalCompanion).hide();
			return DynamicTest.stream(
				Stream.of(
					new ScheduledWithContextInScopeFluxTest<>(operatorUnderTest.apply(hiddenSource, hiddenThreadLocalCompanion), hiddenThreadLocalCompanion, testName),
					new ScheduledWithContextInScopeFluxTest<>(mainSourceAndOperator, mainThreadLocalCompanion, testName + "_fused")
				),
				dynamicTestGenerator
			);
		}
		//otherwise, only add one test
		return DynamicTest.stream(
			Stream.of(new ScheduledWithContextInScopeFluxTest<>(mainSourceAndOperator, mainThreadLocalCompanion, testName)),
			dynamicTestGenerator
		);
	}

	public static final class ThreadLocalCompanion<RES> {

		public final RES                 resource;
		public final ThreadLocal<String> threadLocal;

		ThreadLocalCompanion(RES resource) {
			this.resource = resource;
			threadLocal = ThreadLocal.withInitial(() -> "none");
		}
	}

	public static final class ScheduledWithContextInScopeFluxTest<T, RES> implements Named<ScheduledWithContextInScopeFluxTest<T, RES>> {

		private final Flux<T>                   source;
		private final ThreadLocalCompanion<RES> threadLocalCompanion;
		private final String                    testName;

		public ScheduledWithContextInScopeFluxTest(Flux<T> source, ThreadLocalCompanion<RES> threadLocalCompanion, String name) {
			this.source = source;
			this.threadLocalCompanion = threadLocalCompanion;
			testName = name;
		}

		private void setup() {
			Schedulers.onScheduleContextualHook(testName, (r, c) -> {
				final String fromContext = c.getOrDefault("key", "notFound");
				return () -> {
					threadLocalCompanion.threadLocal.set(fromContext);
					r.run();
					threadLocalCompanion.threadLocal.remove();
				};
			});
		}

		public RES getResource() {
			return this.threadLocalCompanion.resource;
		}

		public StepVerifier.FirstStep<String> mappingTest() {
			setup();
			Flux<String> transformed = source.map(v -> "" + v + threadLocalCompanion.threadLocal.get());
			return StepVerifier.create(transformed, StepVerifierOptions.create().withInitialContext(Context.of("key", "customized")));
		}

		public StepVerifier.FirstStep<T> rawTest() {
			setup();
			return StepVerifier.create(source, StepVerifierOptions.create().withInitialContext(Context.of("key", "customized")));
		}

		@Override
		public String getName() {
			return testName;
		}

		@Override
		public ScheduledWithContextInScopeFluxTest<T, RES> getPayload() {
			return this;
		}
	}
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.micrometer.context.ContextRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class ContextPropagationTest {

	private static final String KEY1 = "key1";
	private static final String KEY2 = "key2";

	private static final AtomicReference<String> REF1 = new AtomicReference<>();
	private static final AtomicReference<String> REF2 = new AtomicReference<>();

	//NOTE: no way to currently remove accessors from the ContextRegistry, so we recreate one on each test
	private ContextRegistry registry;

	@BeforeEach
	void setup() {
		registry = new ContextRegistry().loadContextAccessors();

		REF1.set("ref1_init");
		REF2.set("ref2_init");

		registry.registerThreadLocalAccessor(
			KEY1, REF1::get, REF1::set, () -> REF1.set(null));

		registry.registerThreadLocalAccessor(
			KEY2, REF2::get, REF2::set, () -> REF2.set(null));
	}

	@Test
	void isContextPropagationAvailable() {
		assertThat(ContextPropagation.isContextPropagationAvailable()).isTrue();
	}


	@Test
	void contextCaptureWithNoPredicateReturnsTheConstantFunction() {
		assertThat(ContextPropagation.contextCapture())
			.as("no predicate nor registry")
			.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			.hasFieldOrPropertyWithValue("registry", ContextRegistry.getInstance());
	}

	@Test
	void contextCaptureWithPredicateReturnsNewFunctionWithGlobalRegistry() {
		Function<Context, Context> test = ContextPropagation.contextCapture(ContextPropagation.PREDICATE_TRUE);

		assertThat(test)
			.as("predicate, no registry")
			.isNotNull()
			.isNotSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			.isNotSameAs(ContextPropagation.NO_OP)
			// as long as a predicate is supplied, the method creates new instances of the Function
			.isNotSameAs(ContextPropagation.contextCapture(ContextPropagation.PREDICATE_TRUE))
			.isInstanceOfSatisfying(ContextPropagation.ContextCaptureFunction.class, f ->
				assertThat(f.registry).as("function default registry").isSameAs(ContextRegistry.getInstance()));
	}

	@Test
	void fluxApiUsesContextPropagationConstantFunction() {
		Flux<Integer> source = Flux.empty();
		assertThat(source.contextCapture())
			.isInstanceOfSatisfying(FluxContextWrite.class, fcw ->
				assertThat(fcw.doOnContext)
					.as("flux's capture function")
					.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			);
	}

	@Test
	void monoApiUsesContextPropagationConstantFunction() {
		Mono<Integer> source = Mono.empty();
		assertThat(source.contextCapture())
			.isInstanceOfSatisfying(MonoContextWrite.class, fcw ->
				assertThat(fcw.doOnContext)
					.as("mono's capture function")
					.isSameAs(ContextPropagation.WITH_GLOBAL_REGISTRY_NO_PREDICATE)
			);
	}

	@Nested
	class ContextCaptureFunctionTest {

		@Test
		void contextCaptureFunctionWithoutFiltering() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(
				ContextPropagation.PREDICATE_TRUE, registry);

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
				.containsEntry(KEY1, "ref1_init")
				.containsEntry(KEY2, "ref2_init")
				.hasSize(2);
		}

		@Test
		void captureWithFiltering() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(
				k -> k.toString().equals(KEY2), registry);

			Context ctx = test.apply(Context.empty());
			Map<Object, Object> asMap = new HashMap<>();
			ctx.forEach(asMap::put); //easier to assert

			assertThat(asMap)
				.containsEntry(KEY2, "ref2_init")
				.hasSize(1);
		}

		@Test
		void captureFunctionWithNullRegistryUsesGlobalRegistry() {
			ContextPropagation.ContextCaptureFunction test = new ContextPropagation.ContextCaptureFunction(v -> true, null);

			assertThat(test.registry).as("default registry").isSameAs(ContextRegistry.getInstance());
		}
	}

}

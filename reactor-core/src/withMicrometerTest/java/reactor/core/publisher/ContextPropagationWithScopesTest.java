/*
 * Copyright (c) 2022-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.context.ContextRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import io.micrometer.scopedvalue.Scope;
import io.micrometer.scopedvalue.ScopeHolder;
import io.micrometer.scopedvalue.ScopedValue;
import io.micrometer.scopedvalue.ScopedValueThreadLocalAccessor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dariusz Jędrzejczyk
 */
class ContextPropagationWithScopesTest {

	@BeforeAll
	static void initializeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.registerThreadLocalAccessor(new ScopedValueThreadLocalAccessor());
	}

	@BeforeEach
	void enableHook() {
		Hooks.enableAutomaticContextPropagation();
	}

	@AfterEach
	void cleanupThreadLocals() {
		ScopeHolder.remove();
		Hooks.disableAutomaticContextPropagation();
	}

	@AfterAll
	static void removeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.removeThreadLocalAccessor(ScopedValueThreadLocalAccessor.KEY);
	}

	@Test
	void basicMonoWorks() {
		ScopedValue scopedValue = ScopedValue.create("hello");

		Mono.just("item")
				.doOnNext(item -> assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue))
				.contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, scopedValue))
				.block();

		assertThat(ScopeHolder.currentValue()).isNull();
	}

	@Test
	void basicFluxWorks() {
		ScopedValue scopedValue = ScopedValue.create("hello");

		Flux.just("item")
		    .doOnNext(item -> assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue))
		    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, scopedValue))
		    .blockLast();

		assertThat(ScopeHolder.currentValue()).isNull();
	}

	@Test
	@SuppressWarnings("try")
	void emptyContextWorksInMono() {
		ScopedValue scopedValue = ScopedValue.create("hello");
		try (Scope scope = Scope.open(scopedValue)) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);

			Mono.just("item")
			    .doOnNext(item -> assertThat(ScopeHolder.currentValue().get()).isNull())
			    .contextWrite(ctx -> Context.empty())
			    .block();

			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);
		}

		assertThat(ScopeHolder.currentValue()).isNull();
	}

	@Test
	@SuppressWarnings("try")
	void subscribeMonoElsewhere() {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();

		ScopedValue externalValue = ScopedValue.create("outside");
		ScopedValue internalValue = ScopedValue.create("inside");

		try (Scope scope = Scope.open(externalValue)) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(externalValue);

			Mono.just(1)
			    .subscribeOn(Schedulers.single())
			    .doOnNext(i -> {
				    valueInNewThread.set(ScopeHolder.currentValue());
			    })
			    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, internalValue))
			    .block();

			assertThat(valueInNewThread.get()).isEqualTo(internalValue);
			assertThat(ScopeHolder.currentValue()).isEqualTo(externalValue);
		}

		assertThat(ScopeHolder.currentValue()).isEqualTo(null);
	}

	@Test
	@SuppressWarnings("try")
	void subscribeFluxElsewhere() {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();

		ScopedValue externalValue = ScopedValue.create("outside");
		ScopedValue internalValue = ScopedValue.create("inside");

		try (Scope scope = Scope.open(externalValue)) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(externalValue);

			Flux.just(1)
			    .subscribeOn(Schedulers.single())
			    .doOnNext(i -> {
				    valueInNewThread.set(ScopeHolder.currentValue());
			    })
			    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, internalValue))
			    .blockLast();

			assertThat(valueInNewThread.get()).isEqualTo(internalValue);
			assertThat(ScopeHolder.currentValue()).isEqualTo(externalValue);
		}

		assertThat(ScopeHolder.currentValue()).isEqualTo(null);
	}

	@Test
	@SuppressWarnings("try")
	void multiLevelScopesWithDifferentValuesAndFlux() {
		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		AtomicReference<ScopedValue> valueInsideFlatMap = new AtomicReference<>();

		try (Scope v1scope1 = Scope.open(v1)) {
			try (Scope v1scope2 = Scope.open(v1)) {
				try (Scope v2scope1 = Scope.open(v2)) {
					try (Scope v2scope2 = Scope.open(v2)) {
						try (Scope v1scope3 = Scope.open(v1)) {
							try (Scope nullScope = Scope.open(ScopedValue.nullValue())) {
								assertThat(ScopeHolder.currentValue().get()).isNull();
							}
							assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
							assertThat(ScopeHolder.get()).isEqualTo(v1scope3);

							Flux.just(1)
							    .flatMap(i ->
									    Flux.just(i)
									        .publishOn(Schedulers.boundedElastic())
									        .doOnNext(item -> valueInsideFlatMap.set(ScopeHolder.currentValue())))
							    .blockLast();

							assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
							assertThat(ScopeHolder.get()).isEqualTo(v1scope3);
						}
						assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
						assertThat(ScopeHolder.get()).isEqualTo(v2scope2);
					}
					assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
					assertThat(ScopeHolder.get()).isEqualTo(v2scope1);
				}
				assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
				assertThat(ScopeHolder.get()).isEqualTo(v1scope2);
			}
			assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
			assertThat(ScopeHolder.get()).isEqualTo(v1scope1);
		}

		assertThat(ScopeHolder.currentValue()).isNull();

		assertThat(valueInsideFlatMap.get()).isEqualTo(v1);
	}

	@Test
	@SuppressWarnings("try")
	void multiLevelScopesWithDifferentValuesAndMono() {
		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		AtomicReference<ScopedValue> valueInsideFlatMap = new AtomicReference<>();

		try (Scope v1scope1 = Scope.open(v1)) {
			try (Scope v1scope2 = Scope.open(v1)) {
				try (Scope v2scope1 = Scope.open(v2)) {
					try (Scope v2scope2 = Scope.open(v2)) {
						try (Scope v1scope3 = Scope.open(v1)) {
							try (Scope nullScope = Scope.open(ScopedValue.nullValue())) {
								assertThat(ScopeHolder.currentValue().get()).isNull();
							}
							assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
							assertThat(ScopeHolder.get()).isEqualTo(v1scope3);

							Mono.just(1)
							    .flatMap(i ->
									    Mono.just(i)
									        .publishOn(Schedulers.boundedElastic())
									        .doOnNext(item -> valueInsideFlatMap.set(ScopeHolder.currentValue())))
							    .block();

							assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
							assertThat(ScopeHolder.get()).isEqualTo(v1scope3);
						}
						assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
						assertThat(ScopeHolder.get()).isEqualTo(v2scope2);
					}
					assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
					assertThat(ScopeHolder.get()).isEqualTo(v2scope1);
				}
				assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
				assertThat(ScopeHolder.get()).isEqualTo(v1scope2);
			}
			assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
			assertThat(ScopeHolder.get()).isEqualTo(v1scope1);
		}

		assertThat(ScopeHolder.currentValue()).isNull();

		assertThat(valueInsideFlatMap.get()).isEqualTo(v1);
	}

	@Test
	void parallelFlatMapFlux() {
		ScopedValue outerValue = ScopedValue.create("val1");

		Queue<String> innerValues = new ArrayBlockingQueue<>(3);

		Flux.just(0, 1, 2).hide()
		    .flatMap(i -> Flux.just(i).subscribeOn(Schedulers.parallel())
		                      .doOnNext(item -> {
								  innerValues.offer(ScopeHolder.currentValue().get());
		                      })
		                      .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, ScopedValue.create("item_" + i))),
				    3, 1)
		    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, outerValue))
		    .blockLast();

		assertThat(innerValues).containsExactlyInAnyOrder("item_0", "item_1", "item_2");
		assertThat(ScopeHolder.get()).isNull();
	}
}

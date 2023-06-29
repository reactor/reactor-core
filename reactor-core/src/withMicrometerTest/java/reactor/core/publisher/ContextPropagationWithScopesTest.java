/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.test.ScopedValue;
import io.micrometer.context.test.ScopedValueThreadLocalAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dariusz Jędrzejczyk
 */
class ContextPropagationWithScopesTest {

	private static final Logger log = LoggerFactory.getLogger(ContextPropagationWithScopesTest.class);

	@BeforeAll
	static void initializeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.registerThreadLocalAccessor(new ScopedValueThreadLocalAccessor());
	}

	@BeforeEach
	void enableHook() {
		Hooks.enableAutomaticContextPropagation();
	}

	//the cleanup of "thread locals" could be especially important if one starts relying on
	//the global registry in tests: it would ensure no TL pollution.
	@AfterEach
	void cleanupThreadLocals() {
		ScopedValue.VALUE_IN_SCOPE.remove();
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
				.doOnNext(item -> assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue))
				.contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, scopedValue))
				.block();

		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void basicFluxWorks() {
		ScopedValue scopedValue = ScopedValue.create("hello");

		Flux.just("item")
		    .doOnNext(item -> assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue))
		    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, scopedValue))
		    .blockLast();

		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void emptyContextWorksInMono() {
		ScopedValue scopedValue = ScopedValue.create("hello");
		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);

			Mono.just("item")
			    .doOnNext(item -> assertThat(ScopedValue.getCurrent().get()).isNull())
			    .contextWrite(ctx -> Context.empty())
			    .block();

			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);
		}

		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void subscribeMonoElsewhere() {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();

		ScopedValue externalValue = ScopedValue.create("outside");
		ScopedValue internalValue = ScopedValue.create("inside");

		try (ScopedValue.Scope scope = externalValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(externalValue);

			Mono.just(1)
			    .subscribeOn(Schedulers.single())
			    .doOnNext(i -> {
				    valueInNewThread.set(ScopedValue.getCurrent());
			    })
			    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, internalValue))
			    .block();

			assertThat(valueInNewThread.get()).isEqualTo(internalValue);
			assertThat(ScopedValue.getCurrent()).isEqualTo(externalValue);
		}

		assertThat(ScopedValue.getCurrent()).isEqualTo(null);
	}

	@Test
	void subscribeFluxElsewhere() {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();

		ScopedValue externalValue = ScopedValue.create("outside");
		ScopedValue internalValue = ScopedValue.create("inside");

		try (ScopedValue.Scope scope = externalValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(externalValue);

			Flux.just(1)
			    .subscribeOn(Schedulers.single())
			    .doOnNext(i -> {
				    valueInNewThread.set(ScopedValue.getCurrent());
			    })
			    .contextWrite(Context.of(ScopedValueThreadLocalAccessor.KEY, internalValue))
			    .blockLast();

			assertThat(valueInNewThread.get()).isEqualTo(internalValue);
			assertThat(ScopedValue.getCurrent()).isEqualTo(externalValue);
		}

		assertThat(ScopedValue.getCurrent()).isEqualTo(null);
	}

	@Test
	void multiLevelScopesWithDifferentValuesAndFlux() {
		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		AtomicReference<ScopedValue> valueInsideFlatMap = new AtomicReference<>();

		try (ScopedValue.Scope v1scope1 = v1.open()) {
			try (ScopedValue.Scope v1scope2 = v1.open()) {
				try (ScopedValue.Scope v2scope1 = v2.open()) {
					try (ScopedValue.Scope v2scope2 = v2.open()) {
						try (ScopedValue.Scope v1scope3 = v1.open()) {
							try (ScopedValue.Scope nullScope =
									     ScopedValue.nullValue().open()) {
								assertThat(ScopedValue.getCurrent().get()).isNull();
							}
							assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
							assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope3);

							Flux.just(1)
							    .flatMap(i ->
									    Flux.just(i)
									        .publishOn(Schedulers.boundedElastic())
									        .doOnNext(item -> valueInsideFlatMap.set(ScopedValue.getCurrent())))
							    .blockLast();

							assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
							assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope3);
						}
						assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
						assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope2);
					}
					assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
					assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope1);
				}
				assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
				assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope2);
			}
			assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
			assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope1);
		}

		assertThat(ScopedValue.getCurrent()).isNull();

		assertThat(valueInsideFlatMap.get()).isEqualTo(v1);
	}

	@Test
	void multiLevelScopesWithDifferentValuesAndMono() {
		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		AtomicReference<ScopedValue> valueInsideFlatMap = new AtomicReference<>();

		try (ScopedValue.Scope v1scope1 = v1.open()) {
			try (ScopedValue.Scope v1scope2 = v1.open()) {
				try (ScopedValue.Scope v2scope1 = v2.open()) {
					try (ScopedValue.Scope v2scope2 = v2.open()) {
						try (ScopedValue.Scope v1scope3 = v1.open()) {
							try (ScopedValue.Scope nullScope =
									     ScopedValue.nullValue().open()) {
								assertThat(ScopedValue.getCurrent().get()).isNull();
							}
							assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
							assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope3);

							Mono.just(1)
							    .flatMap(i ->
									    Mono.just(i)
									        .publishOn(Schedulers.boundedElastic())
									        .doOnNext(item -> valueInsideFlatMap.set(ScopedValue.getCurrent())))
							    .block();

							assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
							assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope3);
						}
						assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
						assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope2);
					}
					assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
					assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope1);
				}
				assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
				assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope2);
			}
			assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
			assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v1scope1);
		}

		assertThat(ScopedValue.getCurrent()).isNull();

		assertThat(valueInsideFlatMap.get()).isEqualTo(v1);
	}
}

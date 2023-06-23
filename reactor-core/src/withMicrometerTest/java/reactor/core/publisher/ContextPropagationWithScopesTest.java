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
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.context.ThreadLocalAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
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
	void basicScopeWorks() {
		assertThat(ScopedValue.getCurrent()).isNull();

		ScopedValue scopedValue = ScopedValue.create("hello");
		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);
		}

		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void scopeWorksInAnotherThread() throws Exception {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();
		ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder().build();
		ScopedValue scopedValue = ScopedValue.create("hello");

		assertThat(ScopedValue.getCurrent()).isNull();

		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);
			Runnable wrapped = snapshotFactory.captureAll().wrap(() -> valueInNewThread.set(ScopedValue.getCurrent()));
			Thread t = new Thread(wrapped);
			t.start();
			t.join();
		}

		assertThat(valueInNewThread.get()).isEqualTo(scopedValue);
		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void emptyScopeWorks() {
		assertThat(ScopedValue.getCurrent()).isNull();

		ScopedValue scopedValue = ScopedValue.create("hello");
		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);
			try (ScopedValue.Scope emptyScope = ScopedValue.nullValue().open()) {
				assertThat(ScopedValue.getCurrent()).isInstanceOf(NullScopedValue.class);
				assertThat(ScopedValue.getCurrent().get()).isNull();
			}
			assertThat(ScopedValue.getCurrent()).isEqualTo(scopedValue);
		}

		assertThat(ScopedValue.getCurrent()).isNull();
	}

	@Test
	void nestedScopeWorksInAnotherThread() throws Exception {
		AtomicReference<ScopedValue> value1InNewThreadBefore = new AtomicReference<>();
		AtomicReference<ScopedValue> value1InNewThreadAfter = new AtomicReference<>();
		AtomicReference<ScopedValue> value2InNewThread = new AtomicReference<>();

		ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder().build();

		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		assertThat(ScopedValue.getCurrent()).isNull();

		Thread t;

		try (ScopedValue.Scope v1Scope = v1.open()) {
			assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
			try (ScopedValue.Scope v2scope1T1 = v2.open()) {
				assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
				try (ScopedValue.Scope v2scope2T1 = v2.open()) {
					assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
					Runnable runnable = () -> {
						value1InNewThreadBefore.set(ScopedValue.getCurrent());
						try (ScopedValue.Scope v2scopeT2 = v2.open()) {
							value2InNewThread.set(ScopedValue.getCurrent());
						}
						value1InNewThreadAfter.set(ScopedValue.getCurrent());
					};

					Runnable wrapped = snapshotFactory.captureAll().wrap(runnable);
					t = new Thread(wrapped);
					t.start();

					assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
					assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope2T1);
				}
				assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
				assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(v2scope1T1);
			}

			assertThat(ScopedValue.getCurrent()).isEqualTo(v1);

			try (ScopedValue.Scope childScope3 = v2.open()) {
				assertThat(ScopedValue.getCurrent()).isEqualTo(v2);
				assertThat(ScopedValue.getCurrent().currentScope()).isEqualTo(childScope3);
			}

			t.join();
			assertThat(ScopedValue.getCurrent()).isEqualTo(v1);
		}

		assertThat(value1InNewThreadBefore.get()).isEqualTo(v2);
		assertThat(value1InNewThreadAfter.get()).isEqualTo(v2);
		assertThat(value2InNewThread.get()).isEqualTo(v2);
		assertThat(ScopedValue.getCurrent()).isNull();
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
			    .doOnNext(item -> assertThat(ScopedValue.getCurrent()).isInstanceOf(NullScopedValue.class))
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
	void multiLevelScopesWithDifferentValues() {
		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		try (ScopedValue.Scope v1scope1 = v1.open()) {
			try (ScopedValue.Scope v1scope2 = v1.open()) {
				try (ScopedValue.Scope v2scope1 = v2.open()) {
					try (ScopedValue.Scope v2scope2 = v2.open()) {
						try (ScopedValue.Scope v1scope3 = v1.open()) {
							try (ScopedValue.Scope nullScope =
									     ScopedValue.nullValue().open()) {
								assertThat(ScopedValue.getCurrent()).isInstanceOf(NullScopedValue.class);
								assertThat(ScopedValue.getCurrent().get()).isNull();
							}
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
								assertThat(ScopedValue.getCurrent()).isInstanceOf(NullScopedValue.class);
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
								assertThat(ScopedValue.getCurrent()).isInstanceOf(NullScopedValue.class);
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

	private static class ScopedValueThreadLocalAccessor implements ThreadLocalAccessor<ScopedValue> {

		static final String KEY = "svtla";

		@Override
		public Object key() {
			return KEY;
		}

		@Override
		public ScopedValue getValue() {
			return ScopedValue.VALUE_IN_SCOPE.get();
		}

		@Override
		public void setValue(ScopedValue value) {
			value.open();
		}

		@Override
		public void setValue() {
			ScopedValue.nullValue().open();
		}

		@Override
		public void restore(ScopedValue previousValue) {
			ScopedValue current = ScopedValue.VALUE_IN_SCOPE.get();
			if (current != null) {
				ScopedValue.Scope currentScope = current.currentScope();
				if (currentScope == null || currentScope.parentScope == null ||
						currentScope.parentScope.scopedValue != previousValue) {
					throw new RuntimeException("Restoring to a different previous scope than expected!");
				}
				currentScope.close();
			} else {
				throw new RuntimeException("Restoring to previous scope, but current is missing.");
			}
		}

		@Override
		public void restore() {
			ScopedValue current = ScopedValue.VALUE_IN_SCOPE.get();
			if (current != null) {
				ScopedValue.Scope currentScope = current.currentScope();
				if (currentScope == null) {
					throw new RuntimeException("Can't close current scope, as it is missing");
				}
				currentScope.close();
			} else {
				throw new RuntimeException("Restoring to previous scope, but current is missing.");
			}
		}
	}
	private interface ScopedValue {

		ThreadLocal<ScopedValue> VALUE_IN_SCOPE = new ThreadLocal<>();

		@Nullable
		static ScopedValue getCurrent() {
			return VALUE_IN_SCOPE.get();
		}

		static ScopedValue create(String value) {
			return new SimpleScopedValue(value);
		}

		static ScopedValue nullValue() {
			return new NullScopedValue();
		}

		@Nullable
		String get();

		@Nullable
		Scope currentScope();

		void updateCurrentScope(Scope scope);

		Scope open();

		class Scope implements AutoCloseable {

			private final ScopedValue scopedValue;

			@Nullable
			private final Scope parentScope;

			public Scope(ScopedValue scopedValue) {
				log.info("{}: open scope[{}]", scopedValue.get(), hashCode());
				this.scopedValue = scopedValue;

				ScopedValue currentValue = ScopedValue.VALUE_IN_SCOPE.get();
				this.parentScope = currentValue != null ? currentValue.currentScope() : null;

				ScopedValue.VALUE_IN_SCOPE.set(scopedValue);
			}

			@Override
			public void close() {
				if (parentScope == null) {
					log.info("{}: remove scope[{}]", scopedValue.get(), hashCode());
					ScopedValue.VALUE_IN_SCOPE.remove();
				} else {
					log.info("{}: close scope[{}] -> restore {} scope[{}]",
							scopedValue.get(), hashCode(),
							parentScope.scopedValue.get(), parentScope.hashCode());
					parentScope.scopedValue.updateCurrentScope(parentScope);
					ScopedValue.VALUE_IN_SCOPE.set(parentScope.scopedValue);
				}
			}
		}
	}

	private static class SimpleScopedValue implements ScopedValue {

		private final String value;

		ThreadLocal<Scope> currentScope = new ThreadLocal<>();

		private SimpleScopedValue(String value) {
			this.value = value;
		}

		@Override
		public String get() {
			return value;
		}

		@Override
		public Scope currentScope() {
			return currentScope.get();
		}

		@Override
		public Scope open() {
			Scope scope = new Scope(this);
			this.currentScope.set(scope);
			return scope;
		}

		@Override
		public void updateCurrentScope(Scope scope) {
			log.info("{} update scope[{}] -> scope[{}]",
					value, currentScope.get().hashCode(), scope.hashCode());
			this.currentScope.set(scope);
		}
	}

	private static class NullScopedValue extends SimpleScopedValue {

		public NullScopedValue() {
			super("null");
		}

		@Override
		public String get() {
			return null;
		}
	}
}

/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher.scopedvalue;

import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshotFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// NOTE: These tests are a copy from the context-propagation library. Any changes should
// be considered in the upstream first. Please keep in sync.

public class ScopedValueTest {

	@BeforeAll
	static void initializeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.registerThreadLocalAccessor(new ScopedValueThreadLocalAccessor());
	}

	@AfterEach
	void cleanupThreadLocals() {
		ScopeHolder.remove();
	}

	@AfterAll
	static void removeThreadLocalAccessors() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();
		globalRegistry.removeThreadLocalAccessor(ScopedValueThreadLocalAccessor.KEY);
	}

	@Test
	void basicScopeWorks() {
		assertThat(ScopeHolder.currentValue()).isNull();

		ScopedValue scopedValue = ScopedValue.create("hello");
		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);
		}

		assertThat(ScopeHolder.currentValue()).isNull();
	}

	@Test
	void emptyScopeWorks() {
		assertThat(ScopeHolder.currentValue()).isNull();

		ScopedValue scopedValue = ScopedValue.create("hello");
		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);
			try (ScopedValue.Scope emptyScope = ScopedValue.nullValue().open()) {
				assertThat(ScopeHolder.currentValue().get()).isNull();
			}
			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);
		}

		assertThat(ScopeHolder.currentValue()).isNull();
	}

	@Test
	void scopeWorksInAnotherThread() throws Exception {
		AtomicReference<ScopedValue> valueInNewThread = new AtomicReference<>();
		ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder().build();
		ScopedValue scopedValue = ScopedValue.create("hello");

		assertThat(ScopeHolder.currentValue()).isNull();

		try (ScopedValue.Scope scope = scopedValue.open()) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(scopedValue);
			Runnable wrapped = snapshotFactory.captureAll().wrap(() -> valueInNewThread.set(ScopeHolder.currentValue()));
			Thread t = new Thread(wrapped);
			t.start();
			t.join();
		}

		assertThat(valueInNewThread.get()).isEqualTo(scopedValue);
		assertThat(ScopeHolder.currentValue()).isNull();
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
							try (ScopedValue.Scope nullScope = ScopedValue.nullValue().open()) {
								assertThat(ScopeHolder.currentValue().get()).isNull();
							}
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
	}

	@Test
	void nestedScopeWorksInAnotherThread() throws Exception {
		AtomicReference<ScopedValue> value1InNewThreadBefore = new AtomicReference<>();
		AtomicReference<ScopedValue> value1InNewThreadAfter = new AtomicReference<>();
		AtomicReference<ScopedValue> value2InNewThread = new AtomicReference<>();

		ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder().build();

		ScopedValue v1 = ScopedValue.create("val1");
		ScopedValue v2 = ScopedValue.create("val2");

		assertThat(ScopeHolder.currentValue()).isNull();

		Thread t;

		try (ScopedValue.Scope v1Scope = v1.open()) {
			assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
			try (ScopedValue.Scope v2scope1T1 = v2.open()) {
				assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
				try (ScopedValue.Scope v2scope2T1 = v2.open()) {
					assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
					Runnable runnable = () -> {
						value1InNewThreadBefore.set(ScopeHolder.currentValue());
						try (ScopedValue.Scope v2scopeT2 = v2.open()) {
							value2InNewThread.set(ScopeHolder.currentValue());
						}
						value1InNewThreadAfter.set(ScopeHolder.currentValue());
					};

					Runnable wrapped = snapshotFactory.captureAll().wrap(runnable);
					t = new Thread(wrapped);
					t.start();

					assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
					assertThat(ScopeHolder.get()).isEqualTo(v2scope2T1);
				}
				assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
				assertThat(ScopeHolder.get()).isEqualTo(v2scope1T1);
			}

			assertThat(ScopeHolder.currentValue()).isEqualTo(v1);

			try (ScopedValue.Scope childScope3 = v2.open()) {
				assertThat(ScopeHolder.currentValue()).isEqualTo(v2);
				assertThat(ScopeHolder.get()).isEqualTo(childScope3);
			}

			t.join();
			assertThat(ScopeHolder.currentValue()).isEqualTo(v1);
		}

		assertThat(value1InNewThreadBefore.get()).isEqualTo(v2);
		assertThat(value1InNewThreadAfter.get()).isEqualTo(v2);
		assertThat(value2InNewThread.get()).isEqualTo(v2);
		assertThat(ScopeHolder.currentValue()).isNull();
	}

}

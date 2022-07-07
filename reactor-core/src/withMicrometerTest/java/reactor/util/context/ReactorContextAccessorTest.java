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

package reactor.util.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

/**
 * @author Simon Baslé
 */
class ReactorContextAccessorTest {

	ReactorContextAccessor accessor = new ReactorContextAccessor();

	@Test
	void canReadFromContext() {
		assertThat(accessor.canReadFrom(Context.class)).isTrue();
	}

	@Test
	void canReadFromContextView() {
		assertThat(accessor.canReadFrom(ContextView.class)).isTrue();
	}

	@Test
	void canWriteToContext() {
		assertThat(accessor.canWriteTo(Context.class)).isTrue();
	}

	@Test
	void cannotWriteToContextView() {
		assertThat(accessor.canWriteTo(ContextView.class)).isFalse();
	}

	@Test
	void readValuesUsesPredicateToIncludeValues() {
		Map<Object, Object> target = new HashMap<>();
		ContextView source = Context.of(1, "A", 2, "B", "3", "C");

		accessor.readValues(source, Number.class::isInstance, target);

		assertThat(target)
			.containsOnlyKeys(1, 2)
			.containsEntry(1, "A")
			.containsEntry(2, "B");
	}

	@Test
	void readValuesDoesntUseStream() {
		//we create a ForeignContext, which is a test suite non-final Context implementation
		//so that Mockito will be able to spy it and assert reads don't rely on stream()
		ContextView trueSource = new ContextTest.ForeignContext(1, "A");
		ContextView source = Mockito.spy(trueSource);
		Map<Object, Object> target = new HashMap<>();

		accessor.readValues(source, v -> true, target);

		Mockito.verify(source, never()).stream();
		Mockito.verify(source, times(1)).forEach(any());
	}

	@Test
	void writeValuesWithPutAllMap() {
		//we create a ForeignContext, which is a test suite non-final Context implementation
		//so that Mockito will be able to spy it and assert writes don't rely on putAll()
		Context trueTarget = new ContextTest.ForeignContext(1, "A");
		Context target = Mockito.spy(trueTarget);
		Map<Object, Object> map = Collections.singletonMap(2, "B");

		Context result = accessor.writeValues(map, target);

		assertThat(result)
			.isNotSameAs(trueTarget)
			.isNotSameAs(target);

		assertThat(result.hasKey(2)).as("contains key 2").isTrue();
		Object value2 = result.get(2);
		assertThat(value2).as("value for key 2").isEqualTo("B");

		Mockito.verify(target, never()).putAll((ContextView) any());
		Mockito.verify(target, times(1)).putAllMap(anyMap());
	}
}

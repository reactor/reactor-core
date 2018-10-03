/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.context;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ContextUnsupportedTest {

	RuntimeException cause;
	Context c;

	@Before
	public void initUnsupported() {
		cause = new RuntimeException("ContextUnsupportedTest");
		c = Context.unsupported(cause);
	}

	@Test
	public void putAnyKeyContext1() {
		Context put = c.put(1, "Abis");
		assertThat(put)
				.isInstanceOf(Context1.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("Abis");
	}

	@Test
	public void isEmpty() {
		assertThat(Context.unsupported(cause).isEmpty()).as("unsupported().isEmpty()").isTrue();
		assertThat(new ContextUnsupported(cause).isEmpty()).as("new ContextUnsupported().isEmpty()").isTrue();
	}

	@Test
	public void hasKey() {
		assertThat(c.hasKey(1)).as("hasKey(1)").isFalse();
	}

	@Test
	public void removeKeys() {
		assertThat(c.delete(1)).isSameAs(c);
	}

	@Test
	public void getThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.get(1))
				.withMessage("Context#get(Object) is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getClassThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.get(String.class))
				.withMessage("Context#get(Class) is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrEmptyThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrEmpty(1))
		.withMessage("Context#getOrEmpty is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrDefaultWithDefaultThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrDefault("peeka", "boo"))
		.withMessage("Context#getOrDefault is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrDefaultWithDefaultNullThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrDefault("peeka", null))
				.withMessage("Context#getOrDefault is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void stream() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.stream())
		.withMessage("Context#stream is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void string() {
		assertThat(c.toString()).isEqualTo("ContextUnsupported{}");
	}

	@Test
	public void emptyApi() {
		assertThat(Context.empty())
				.isInstanceOf(Context0.class)
				.hasToString("Context0{}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(Context3.class)
		               .hasToString("Context3{A=1, B=2, C=3}");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

}
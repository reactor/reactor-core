/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.util.context.ContextTest.key;
import static reactor.util.context.ContextTest.keyValue;

public class Context2Test {

	Context2 c = new Context2(1, "A", 2, "B");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(Context2.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo", "B");
	}

	@Test
	public void replaceKey2NewContext() {
		Context put = c.put(2, "foo");

		assertThat(put)
				.isInstanceOf(Context2.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "foo");
	}

	@Test
	public void putDifferentKeyContext3() throws Exception {
		Context put = c.put(3, "Abis");
		assertThat(put)
				.isInstanceOf(Context3.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isTrue();
		assertThat(c.hasKey(3)).as("hasKey(3)").isFalse();
	}

	@Test
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context1.class)
				.has(keyValue(2, "B"))
				.doesNotHave(key(1));

		assertThat(c.delete(2))
				.as("delete(2)")
				.isInstanceOf(Context1.class)
				.has(keyValue(1, "A"))
				.doesNotHave(key(2));

		assertThat(c.delete(3)).isSameAs(c);
	}

	@Test
	public void get() {
		assertThat((String) c.get(1)).isEqualTo("A");
		assertThat((String) c.get(2)).isEqualTo("B");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(3))
				.withMessage("Context does not contain key: 3");
	}

	@Test
	public void getUnknownWithDefault() throws Exception {
		assertThat(c.getOrDefault("peeka", "boo")).isEqualTo("boo");
	}

	@Test
	public void getUnknownWithDefaultNull() throws Exception {
		Object def = null;
		assertThat(c.getOrDefault("peeka", def)).isNull();
	}

	@Test
	public void stream() throws Exception {
		assertThat(c.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
				.hasSize(2)
				.containsOnlyKeys(1, 2)
				.containsValues("A", "B");
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context2{1=A, 2=B}");
	}

	@Test
	public void ofApi() {
		assertThat(Context.of("test", 12, "value", true))
				.isInstanceOf(Context2.class)
				.hasToString("Context2{test=12, value=true}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(Context5.class)
		               .hasToString("Context5{1=A, 2=B, A=1, B=2, C=3}");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}
}
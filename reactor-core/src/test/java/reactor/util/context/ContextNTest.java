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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ContextNTest {

	ContextN c = new ContextN(1, "A", 2, "B", 3, "C",
			4, "D", 5, "E", 6, "F");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo", "B", "C", "D", "E", "F");
	}

	@Test
	public void replaceKey2NewContext() {
		Context put = c.put(2, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "foo", "C", "D", "E", "F");
	}

	@Test
	public void replaceKey3NewContext() {
		Context put = c.put(3, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "foo", "D", "E", "F");
	}

	@Test
	public void replaceKey4NewContext() {
		Context put = c.put(4, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "foo", "E", "F");
	}

	@Test
	public void replaceKey5NewContext() {
		Context put = c.put(5, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "foo", "F");
	}

	@Test
	public void replaceKey6NewContext() {
		Context put = c.put(6, "foo");

		assertThat(put)
				.isInstanceOf(ContextN.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "E", "foo");
	}

	@Test
	public void putDifferentKeyContextN() throws Exception {
		Context put = c.put(7, "Abis");
		assertThat(put)
				.isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6, 7);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "E", "F", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isTrue();
		assertThat(c.hasKey(3)).as("hasKey(3)").isTrue();
		assertThat(c.hasKey(4)).as("hasKey(4)").isTrue();
		assertThat(c.hasKey(5)).as("hasKey(5)").isTrue();
		assertThat(c.hasKey(6)).as("hasKey(6)").isTrue();
		assertThat(c.hasKey(7)).as("hasKey(7)").isFalse();
	}

	@Test
	public void get() throws Exception {
		assertThat(c.get(1)).isEqualTo("A");
		assertThat(c.get(2)).isEqualTo("B");
		assertThat(c.get(3)).isEqualTo("C");
		assertThat(c.get(4)).isEqualTo("D");
		assertThat(c.get(5)).isEqualTo("E");
		assertThat(c.get(6)).isEqualTo("F");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(7))
				.withMessage("Context does not contain key: 7");
	}

	@Test
	public void getUnknownWithDefault() throws Exception {
		assertThat(c.getOrDefault(7, "boo")).isEqualTo("boo");
	}

	@Test
	public void stream() throws Exception {
		assertThat(c.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
				.hasSize(6)
				.containsOnlyKeys(1, 2, 3, 4, 5, 6)
				.containsValues("A", "B", "C", "D", "E", "F");
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("ContextN{1=A, 2=B, 3=C, 4=D, 5=E, 6=F}");
	}

	@Test
	public void putAllOfContext3() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, "A", "B", "C");
	}

	@Test
	public void putAllOfContextN() {
		Context m = new ContextN("A", 1, "B", 2, "C", 3,
				"D", 1, "E", 2, "F", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, "A", "B", "C", "D", "E", "F");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

}
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class Context5Test {

	Context5 c = new Context5(1, "A", 2, "B", 3, "C",
			4, "D", 5, "E");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(Context5.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo", "B", "C", "D", "E");
	}

	@Test
	public void replaceKey2NewContext() {
		Context put = c.put(2, "foo");

		assertThat(put)
				.isInstanceOf(Context5.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "foo", "C", "D", "E");
	}

	@Test
	public void replaceKey3NewContext() {
		Context put = c.put(3, "foo");

		assertThat(put)
				.isInstanceOf(Context5.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "foo", "D", "E");
	}

	@Test
	public void replaceKey4NewContext() {
		Context put = c.put(4, "foo");

		assertThat(put)
				.isInstanceOf(Context5.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "foo", "E");
	}

	@Test
	public void replaceKey5NewContext() {
		Context put = c.put(5, "foo");

		assertThat(put)
				.isInstanceOf(Context5.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "foo");
	}

	@Test
	public void putDifferentKeyContextN() throws Exception {
		Context put = c.put(6, "Abis");
		assertThat(put)
				.isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5, 6);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "E", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isTrue();
		assertThat(c.hasKey(3)).as("hasKey(3)").isTrue();
		assertThat(c.hasKey(4)).as("hasKey(4)").isTrue();
		assertThat(c.hasKey(5)).as("hasKey(5)").isTrue();
		assertThat(c.hasKey(6)).as("hasKey(6)").isFalse();
	}

	@Test
	public void get() throws Exception {
		//TODO meh, necessary cast to Object
		assertThat((Object) c.get(1)).isEqualTo("A");
		assertThat((Object) c.get(2)).isEqualTo("B");
		assertThat((Object) c.get(3)).isEqualTo("C");
		assertThat((Object) c.get(4)).isEqualTo("D");
		assertThat((Object) c.get(5)).isEqualTo("E");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(6))
				.withMessage("Context does not contain key: 6");
	}

	@Test
	public void getUnknownWithDefault() throws Exception {
		assertThat(c.getOrDefault(6, "boo")).isEqualTo("boo");
	}

	@Test
	public void stream() throws Exception {
		assertThat(c.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
				.hasSize(5)
				.containsOnlyKeys(1, 2, 3, 4, 5)
				.containsValues("A", "B", "C", "D", "E");
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context5{1=A, 2=B, 3=C, 4=D, 5=E}");
	}

	@Test
	public void ofApi() {
		Date d = new Date();
		assertThat(Context.of("test", 12, "value", true,
				123, 456L, true, false, d, "today"))
				.isInstanceOf(Context5.class)
				.hasToString("Context5{test=12, value=true, 123=456, true=false, " + d + "=today}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, 4, 5, "A", "B", "C");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

}
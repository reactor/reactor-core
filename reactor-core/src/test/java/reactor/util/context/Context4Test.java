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

public class Context4Test {

	Context4 c = new Context4(1, "A", 2, "B", 3, "C", 4, "D");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(Context4.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo", "B", "C", "D");
	}

	@Test
	public void replaceKey2NewContext() {
		Context put = c.put(2, "foo");

		assertThat(put)
				.isInstanceOf(Context4.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "foo", "C", "D");
	}

	@Test
	public void replaceKey3NewContext() {
		Context put = c.put(3, "foo");

		assertThat(put)
				.isInstanceOf(Context4.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "foo", "D");
	}

	@Test
	public void replaceKey4NewContext() {
		Context put = c.put(4, "foo");

		assertThat(put)
				.isInstanceOf(Context4.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "foo");
	}

	@Test
	public void putDifferentKeyContext5() throws Exception {
		Context put = c.put(5, "Abis");
		assertThat(put)
				.isInstanceOf(Context5.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4, 5);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "D", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isTrue();
		assertThat(c.hasKey(3)).as("hasKey(3)").isTrue();
		assertThat(c.hasKey(4)).as("hasKey(4)").isTrue();
		assertThat(c.hasKey(5)).as("hasKey(5)").isFalse();
	}

	@Test
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context3.class)
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.doesNotHave(key(1));

		assertThat(c.delete(2))
				.as("delete(2)")
				.isInstanceOf(Context3.class)
				.has(keyValue(1, "A"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.doesNotHave(key(2));

		assertThat(c.delete(3))
				.as("delete(3)")
				.isInstanceOf(Context3.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(4, "D"))
				.doesNotHave(key(3));

		assertThat(c.delete(4))
				.as("delete(4)")
				.isInstanceOf(Context3.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.doesNotHave(key(4));

		assertThat(c.delete(5)).isSameAs(c);
	}

	@Test
	public void get() throws Exception {
		//TODO meh, necessary cast to Object
		assertThat((Object) c.get(1)).isEqualTo("A");
		assertThat((Object) c.get(2)).isEqualTo("B");
		assertThat((Object) c.get(3)).isEqualTo("C");
		assertThat((Object) c.get(4)).isEqualTo("D");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(5))
				.withMessage("Context does not contain key: 5");
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
				.hasSize(4)
				.containsOnlyKeys(1, 2, 3, 4)
				.containsValues("A", "B", "C", "D");
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context4{1=A, 2=B, 3=C, 4=D}");
	}

	@Test
	public void ofApi() {
		assertThat(Context.of("test", 12, "value", true, 123, 456L, true, false))
				.isInstanceOf(Context4.class)
				.hasToString("Context4{test=12, value=true, 123=456, true=false}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, 4, "A", "B", "C");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

}
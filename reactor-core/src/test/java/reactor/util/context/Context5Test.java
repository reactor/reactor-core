/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.util.context.ContextTest.key;
import static reactor.util.context.ContextTest.keyValue;

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
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context4.class)
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.doesNotHave(key(1));

		assertThat(c.delete(2))
				.as("delete(2)")
				.isInstanceOf(Context4.class)
				.has(keyValue(1, "A"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.doesNotHave(key(2));

		assertThat(c.delete(3))
				.as("delete(3)")
				.isInstanceOf(Context4.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.doesNotHave(key(3));

		assertThat(c.delete(4))
				.as("delete(4)")
				.isInstanceOf(Context4.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(5, "E"))
				.doesNotHave(key(4));

		assertThat(c.delete(5))
				.as("delete(5)")
				.isInstanceOf(Context4.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.doesNotHave(key(5));

		assertThat(c.delete(6)).isSameAs(c);
	}

	@Test
	public void get() {
		assertThat((String) c.get(1)).isEqualTo("A");
		assertThat((String) c.get(2)).isEqualTo("B");
		assertThat((String) c.get(3)).isEqualTo("C");
		assertThat((String) c.get(4)).isEqualTo("D");
		assertThat((String) c.get(5)).isEqualTo("E");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(6))
				.withMessage("Context does not contain key: 6");
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
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, 4, 5, "A", "B", "C");
	}

	@Test
	public void putAllReplaces() {
		Context m = Context.of(c.key1, "replaced", "A", 1);
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(ContextN.class)
		               .hasToString("ContextN{1=replaced, 2=B, 3=C, 4=D, 5=E, A=1}");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m.readOnly());

		assertThat(put).isSameAs(c);
	}

	@Test
	public void putNonNullWithNull() {
		Context put = c.putNonNull("putNonNull", null);

		assertThat(put).isSameAs(c);
	}

	@Test
	public void putNonNullWithValue() {
		Context put = c.putNonNull("putNonNull", "value");

		assertThat(put.getOrEmpty("putNonNull")).contains("value");
	}

	@Test
	public void size() {
		assertThat(c.size()).isEqualTo(5);
	}

	@Test
	public void putAllSelfIntoEmpty() {
		CoreContext initial = new Context0();

		Context result = ((CoreContext) c).putAllInto(initial);

		assertThat(result).isNotSameAs(initial)
		                  .isNotSameAs(c);

		assertThat(result.stream()).containsExactlyElementsOf(c.stream().collect(Collectors.toList()));
	}

	@Test
	public void putAllSelfIntoContextN() {
		CoreContext initial = new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6);
		Context5 self = new Context5("A", 1, "B", 2, "C", 3, "D", 4, "E", 5);
		Context result = self.putAllInto(initial);

		assertThat(result).isNotSameAs(initial)
		                  .isNotSameAs(c);

		assertThat(result.stream().map(String::valueOf))
				.containsExactly("1=1", "2=2", "3=3", "4=4", "5=5", "6=6", "A=1", "B=2", "C=3", "D=4", "E=5");
	}

	@Test
	public void unsafePutAllIntoShouldReplace() {
		ContextN ctx = new ContextN(Collections.emptyMap());
		ctx.accept(1, "VALUE1");
		ctx.accept(2, "VALUE2");
		ctx.accept(3, "VALUE3");
		ctx.accept(4, "VALUE4");
		ctx.accept(5, "VALUE5");
		ctx.accept("extra", "value");

		Context5 self = new Context5(1, "REPLACED1", 2, "REPLACED2",
				3, "REPLACED3", 4, "REPLACED4", 5, "REPLACED5");

		self.unsafePutAllInto(ctx);

		assertThat(ctx)
				.containsEntry(1, "REPLACED1")
				.containsEntry(2, "REPLACED2")
				.containsEntry(3, "REPLACED3")
				.containsEntry(4, "REPLACED4")
				.containsEntry(5, "REPLACED5")
				.containsEntry("extra", "value")
				.hasSize(6);
	}
}

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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.util.context.ContextTest.key;
import static reactor.util.context.ContextTest.keyValue;

public class Context3Test {

	Context3 c = new Context3(1, "A", 2, "B", 3, "C");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(Context3.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo", "B", "C");
	}

	@Test
	public void replaceKey2NewContext() {
		Context put = c.put(2, "foo");

		assertThat(put)
				.isInstanceOf(Context3.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "foo", "C");
	}

	@Test
	public void replaceKey3NewContext() {
		Context put = c.put(3, "foo");

		assertThat(put)
				.isInstanceOf(Context3.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "foo");
	}

	@Test
	public void putDifferentKeyContext4() throws Exception {
		Context put = c.put(4, "Abis");
		assertThat(put)
				.isInstanceOf(Context4.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2, 3, 4);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "B", "C", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isTrue();
		assertThat(c.hasKey(3)).as("hasKey(3)").isTrue();
		assertThat(c.hasKey(4)).as("hasKey(4)").isFalse();
	}

	@Test
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context2.class)
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.doesNotHave(key(1));

		assertThat(c.delete(2))
				.as("delete(2)")
				.isInstanceOf(Context2.class)
				.has(keyValue(1, "A"))
				.has(keyValue(3, "C"))
				.doesNotHave(key(2));

		assertThat(c.delete(3))
				.as("delete(3)")
				.isInstanceOf(Context2.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.doesNotHave(key(3));

		assertThat(c.delete(4)).isSameAs(c);
	}

	@Test
	public void get() {
		assertThat((String) c.get(1)).isEqualTo("A");
		assertThat((String) c.get(2)).isEqualTo("B");
		assertThat((String) c.get(3)).isEqualTo("C");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(4))
				.withMessage("Context does not contain key: 4");
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
				.hasSize(3)
				.containsOnlyKeys(1, 2, 3)
				.containsValues("A", "B", "C");
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context3{1=A, 2=B, 3=C}");
	}

	@Test
	public void ofApi() {
		assertThat(Context.of("test", 12, "value", true, 123, 456L))
				.isInstanceOf(Context3.class)
				.hasToString("Context3{test=12, value=true, 123=456}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(ContextN.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactlyInAnyOrder(1, 2, 3, "A", "B", "C");
	}

	@Test
	public void putAllReplaces() {
		Context m = Context.of(c.key1, "replaced", "A", 1);
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(Context4.class)
		               .hasToString("Context4{1=replaced, 2=B, 3=C, A=1}");
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
		assertThat(c.size()).isEqualTo(3);
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
		Context3 self = new Context3("A", 1, "B", 2, "C", 3);
		Context result = self.putAllInto(initial);

		assertThat(result).isNotSameAs(initial)
		                  .isNotSameAs(c);

		assertThat(result.stream().map(String::valueOf))
				.containsExactly("1=1", "2=2", "3=3", "4=4", "5=5", "6=6", "A=1", "B=2", "C=3");
	}

	@Test
	public void unsafePutAllIntoShouldReplace() {
		ContextN ctx = new ContextN(Collections.emptyMap());
		ctx.accept(1, "VALUE1");
		ctx.accept(2, "VALUE2");
		ctx.accept(3, "VALUE3");
		ctx.accept("extra", "value");

		Context3 self = new Context3(1, "REPLACED1", 2, "REPLACED2",
				3, "REPLACED3");

		self.unsafePutAllInto(ctx);

		assertThat(ctx)
				.containsEntry(1, "REPLACED1")
				.containsEntry(2, "REPLACED2")
				.containsEntry(3, "REPLACED3")
				.containsEntry("extra", "value")
				.hasSize(4);
	}
}

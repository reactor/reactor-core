/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.util.context.ContextTest.key;

public class Context1Test {

	Context1 c = new Context1(1, "A");

	@Test
	public void replaceKey1NewContext() throws Exception {
		Context put = c.put(1, "foo");

		assertThat(put)
				.isInstanceOf(Context1.class)
				.isNotSameAs(c);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("foo");
	}

	@Test
	public void putDifferentKeyContext2() throws Exception {
		Context put = c.put(2, "Abis");
		assertThat(put)
				.isInstanceOf(Context2.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1, 2);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("A", "Abis");
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isTrue();
		assertThat(c.hasKey(2)).as("hasKey(2)").isFalse();
	}


	@Test
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context0.class)
				.doesNotHave(key(1));

		assertThat(c.delete(2)).isSameAs(c);
	}

	@Test
	public void get() {
		assertThat((String) c.get(1)).isEqualTo("A");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(2))
				.withMessage("Context does not contain key: 2");
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
				.hasSize(1)
				.containsOnlyKeys(1)
				.containsValues("A");
	}

	@Test
	void forEach() {
		Map<Object, Object> items = new HashMap<>();

		c.forEach(items::put);

		assertThat(items)
				.hasSize(1)
				.containsOnlyKeys(1)
				.containsValues("A");
	}

	@Test
	void forEachThrows() {
		Map<Object, Object> items = new HashMap<>();

		BiConsumer<Object, Object> action = (key, value) -> {
			if (key.equals(1)) {
				throw new RuntimeException("Boom!");
			}
			items.put(key, value);
		};

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> c.forEach(action))
				.withMessage("Boom!");

		assertThat(items).isEmpty();
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context1{1=A}");
	}

	@Test
	public void ofApi() {
		assertThat(Context.of("test", 12))
				.isInstanceOf(Context1.class)
				.hasToString("Context1{test=12}");
	}

	@Test
	public void putAllOf() {
		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(Context4.class)
		               .hasToString("Context4{1=A, A=1, B=2, C=3}");
	}

	@Test
	public void putAllReplaces() {
		Context m = Context.of(c.key, "replaced", "A", 1);
		Context put = c.putAll(m.readOnly());

		assertThat(put).isInstanceOf(Context2.class)
		               .hasToString("Context2{1=replaced, A=1}");
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
		assertThat(c.size()).isOne();
	}

	@Test
	public void streamHasCleanToString() {
		Context1 context1 = new Context1("key", "value");

		assertThat(context1.toString()).as("toString").isEqualTo("Context1{key=value}");

		assertThat(context1.stream().map(Objects::toString).collect(Collectors.joining(", ")))
				.as("stream elements representation")
				.isEqualTo("key=value");
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
		Context1 self = new Context1("A", 1);
		Context result = self.putAllInto(initial);

		assertThat(result).isNotSameAs(initial)
		                  .isNotSameAs(c);

		assertThat(result.stream().map(String::valueOf))
				.containsExactly("1=1", "2=2", "3=3", "4=4", "5=5", "6=6", "A=1");
	}

	@Test
	public void unsafePutAllIntoShouldReplace() {
		ContextN ctx = new ContextN(Collections.emptyMap());
		ctx.accept(1, "VALUE1");
		ctx.accept("extra", "value");

		Context1 self = new Context1(1, "REPLACED");

		self.unsafePutAllInto(ctx);

		assertThat(ctx)
				.containsEntry(1, "REPLACED")
				.containsEntry("extra", "value")
				.hasSize(2);
	}

	@Test
	void putAllMap() {
		Map<Object, Object> map = new HashMap<>();
		map.put("A", 1);
		map.put("B", 2);
		map.put("C", 3);
		Context put = c.putAllMap(map);

		assertThat(put).isInstanceOf(Context4.class)
				.hasToString("Context4{1=A, A=1, B=2, C=3}");
	}

	@Test
	void putAllMapEmpty() {
		Context put = c.putAllMap(Collections.emptyMap());
		assertThat(put).isSameAs(c);
	}

	@Test
	void putAllMapNullKey() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> c.putAllMap(Collections.singletonMap(null, "oops")));
	}

	@Test
	void putAllMapNullValue() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> c.putAllMap(Collections.singletonMap("A", null)));
	}

	@Test
	void putAllMapReplaces() {
		Map<Object, Object> map = new HashMap<>();
		map.put(c.key, "replaced");
		map.put("A", 1);
		Context put = c.putAllMap(map);

		assertThat(put).isInstanceOf(Context2.class)
				.hasToString("Context2{1=replaced, A=1}");
	}
}

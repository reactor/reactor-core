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
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static reactor.util.context.ContextTest.*;

public class ContextNTest {

	ContextN c;

	@Before
	public void initContext() {
		c = new ContextN(1, "A", 2, "B", 3, "C",
			4, "D", 5, "E", 6, "F");
	}

	@Test
	public void constructFromPairsRejectsNulls() {
		assertThatNullPointerException().isThrownBy(() -> new ContextN(null, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, null, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, null, 2, 3, 3, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, null, 3, 3, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, null, 3, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, null, 4, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, null, 4, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, 4, null, 5, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, 4, 4, null, 5, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, null, 6, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, null, 6));
		assertThatNullPointerException().isThrownBy(() -> new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, null));
	}

	@Test
	public void constructFromPairsConsistent() {
		ContextN contextN = new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6);

		assertThat(contextN)
				.containsKeys(1, 2, 3, 4, 5, 6)
				.containsValues(1, 2, 3, 4 ,5 ,6);
	}

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
	public void removeKeys() {
		assertThat(c.delete(1))
				.as("delete(1)")
				.isInstanceOf(Context5.class)
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.has(keyValue(6, "F"))
				.doesNotHave(key(1));

		assertThat(c.delete(2))
				.as("delete(2)")
				.isInstanceOf(Context5.class)
				.has(keyValue(1, "A"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.has(keyValue(6, "F"))
				.doesNotHave(key(2));

		assertThat(c.delete(3))
				.as("delete(3)")
				.isInstanceOf(Context5.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.has(keyValue(6, "F"))
				.doesNotHave(key(3));

		assertThat(c.delete(4))
				.as("delete(4)")
				.isInstanceOf(Context5.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(5, "E"))
				.has(keyValue(6, "F"))
				.doesNotHave(key(4));

		assertThat(c.delete(5))
				.as("delete(5)")
				.isInstanceOf(Context5.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(6, "F"))
				.doesNotHave(key(5));

		assertThat(c.delete(6))
				.as("delete(6)")
				.isInstanceOf(Context5.class)
				.has(keyValue(1, "A"))
				.has(keyValue(2, "B"))
				.has(keyValue(3, "C"))
				.has(keyValue(4, "D"))
				.has(keyValue(5, "E"))
				.doesNotHave(key(6));

		assertThat(c.delete(7)).isSameAs(c);

		assertThat(c).hasSize(6); //sanity check size unchanged for c
	}

	@Test
	public void removeKeysOver6ReturnsSame() {
		Context largerThan6 = c.put(100, 200);
		assertThat(largerThan6)
				.isNotSameAs(c)
				.has(size(7));

		Context test = largerThan6.delete(6);

		assertThat(test)
				.has(size(6))
				.isNotSameAs(c)
				.isNotSameAs(largerThan6)
				.has(key(1))
				.has(key(2))
				.has(key(3))
				.has(key(4))
				.has(key(5))
				.has(key(100))
				.doesNotHave(key(6));
	}

	@Test
	public void get() {
		assertThat((String) c.get(1)).isEqualTo("A");
		assertThat((String) c.get(2)).isEqualTo("B");
		assertThat((String) c.get(3)).isEqualTo("C");
		assertThat((String) c.get(4)).isEqualTo("D");
		assertThat((String) c.get(5)).isEqualTo("E");
		assertThat((String) c.get(6)).isEqualTo("F");
	}

	@Test
	public void getUnknown() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(7))
				.withMessage("Context does not contain key: 7");
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
	public void putAllReplaces() {
		Context m = Context.of(1, "replaced", "A", 1);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(ContextN.class)
		               .hasToString("ContextN{1=replaced, 2=B, 3=C, 4=D, 5=E, 6=F, A=1}");
	}

	@Test
	public void putAllOfEmpty() {
		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

	@Test
	public void putAllForeign() {
		ForeignContext other = new ForeignContext("someKey", "someValue");
		Context result = c.putAll(other);

		assertThat(result).isInstanceOf(ContextN.class);

		ContextN resultN = (ContextN) result;

		assertThat(resultN)
				.isNotSameAs(c)
				.containsKeys(1, 2, 3, 4, 5, 6, "someKey")
				.containsValues("A", "B", "C", "D", "E", "F", "someValue");
	}

	@Test
	public void putNonNullWithNull() {
		int expectedSize = c.size();
		Context put = c.putNonNull("putNonNull", null);

		assertThat(put).isSameAs(c);
		assertThat(put.size()).isEqualTo(expectedSize);
	}

	@Test
	public void putNonNullWithValue() {
		int expectedSize = c.size() + 1;
		Context put = c.putNonNull("putNonNull", "value");

		assertThat(put).isNotSameAs(c);
		assertThat(put.getOrEmpty("putNonNull")).contains("value");
		assertThat(put.size()).isEqualTo(expectedSize);
	}

	@Test
	public void contextSize() { //renamed to allow import of size() Condition from ContextTest
		assertThat(c.size()).isEqualTo(6);

		assertThat(c.put("sizeGrows", "yes").size()).isEqualTo(7);
	}

	@Test
	public void streamIsNotMutable() {
		c.stream().forEach(e -> { try { e.setValue("REPLACED"); } catch (UnsupportedOperationException ignored) { } });

		assertThat(c).doesNotContainValue("REPLACED");
	}

	@Test
	public void streamHasCleanToString() {
		assertThat(c.toString()).as("toString").isEqualTo("ContextN{1=A, 2=B, 3=C, 4=D, 5=E, 6=F}");

		assertThat(c.stream().map(Objects::toString).collect(Collectors.joining(", ")))
				.as("stream elements representation")
				.isEqualTo("1=A, 2=B, 3=C, 4=D, 5=E, 6=F");
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
		ContextN self = new ContextN("A", 1, "B", 2, "C", 3, "D", 4, "E", 5, "F", 6);
		Context result = self.putAllInto(initial);

		assertThat(result).isNotSameAs(initial)
		                  .isNotSameAs(c);

		assertThat(result.stream().map(String::valueOf))
				.containsExactly("1=1", "2=2", "3=3", "4=4", "5=5", "6=6", "A=1", "B=2", "C=3", "D=4", "E=5", "F=6");
	}

	@Test
	public void shouldNotMutateOriginalMap() {
		Map<Object, Object> original = new HashMap<>();
		original.put("A", 1);
		ContextN contextN = new ContextN(original);
		contextN.accept("A", -1);

		assertThat(original).containsEntry("A", 1);
		assertThat(contextN).containsEntry("A", -1);
	}

}
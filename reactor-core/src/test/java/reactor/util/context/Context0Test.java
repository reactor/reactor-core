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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class Context0Test {

	Context c = Context.empty();

	@Test
	public void putAnyKeyContext1() throws Exception {
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
		assertThat(Context.empty().isEmpty()).as("empty().isEmpty()").isTrue();
		assertThat(new Context0().isEmpty()).as("new Context0().isEmpty()").isTrue();
	}

	@Test
	public void hasKey() throws Exception {
		assertThat(c.hasKey(1)).as("hasKey(1)").isFalse();
	}

	@Test
	public void removeKeys() {
		assertThat(c.delete(1)).isSameAs(c);
	}

	@Test
	public void getThrows() throws Exception {
		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(1))
				.withMessage("Context is empty");
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
		assertThat(c.stream()).isEmpty();
	}

	@Test
	public void string() throws Exception {
		assertThat(c.toString()).isEqualTo("Context0{}");
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
		assertThat(c.size()).isZero();
	}

	@Test
	public void putAllSelfIntoEmptyReturnsSame() {
		CoreContext initial = new Context0();

		Context result = ((CoreContext) c).putAllInto(initial);

		assertThat(result).isSameAs(initial);
	}

	@Test
	public void putAllSelfIntoContextNReturnsSame() {
		CoreContext initial = new ContextN(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6);
		Context0 self = new Context0();
		Context result = self.putAllInto(initial);

		assertThat(result).isSameAs(initial);
	}

	@Test
	public void unsafePutAllIntoIsNoOp() {
		ContextN ctx = new ContextN(Collections.emptyMap());
		ctx.accept(1, "SHOULD NOT BE REPLACED");

		Context0 self = new Context0();

		self.unsafePutAllInto(ctx);

		assertThat(ctx)
				.containsEntry(1, "SHOULD NOT BE REPLACED")
				.hasSize(1);
	}
}
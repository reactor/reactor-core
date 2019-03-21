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

import org.assertj.core.api.Condition;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ContextTest {

	static Condition<Context> key(Object k) {
		return new Condition<>(c -> c.hasKey(k), "key <%s>", k);
	}

	static Condition<Context> keyValue(Object k, Object value) {
		return new Condition<>(c -> c.getOrEmpty(k)
		                             .map(v -> v.equals(value))
		                             .orElse(false),
				"key <%s> with value <%s>", k, value);
	}

	private static final Condition<Context> SIZE_0 = new Condition<>(c -> c instanceof Context0, "size 0");
	private static final Condition<Context> SIZE_1 = new Condition<>(c -> c instanceof Context1, "size 1");
	private static final Condition<Context> SIZE_2 = new Condition<>(c -> c instanceof Context2, "size 2");
	private static final Condition<Context> SIZE_3 = new Condition<>(c -> c instanceof Context3, "size 3");
	private static final Condition<Context> SIZE_4 = new Condition<>(c -> c instanceof Context4, "size 4");
	private static final Condition<Context> SIZE_5 = new Condition<>(c -> c instanceof Context5, "size 5");

	static Condition<Context> size(int n) {
		switch (n) {
			case 0: return SIZE_0;
			case 1: return SIZE_1;
			case 2: return SIZE_2;
			case 3: return SIZE_3;
			case 4: return SIZE_4;
			case 5: return SIZE_5;
			default: return new Condition<>(c -> c instanceof ContextN
					&& c.stream().count() == n,
					"size %d", n);
		}
	}

	@Test
	public void empty() throws Exception {
		Context c = Context.empty();

		assertThat(c).isInstanceOf(Context0.class);
		assertThat(c.isEmpty()).as("isEmpty").isTrue();
	}

	@Test
	public void of1() throws Exception {
		Context c = Context.of(1, 100);

		assertThat(c).isInstanceOf(Context1.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(1);
	}

	@Test
	public void of2() throws Exception {
		Context c = Context.of(1, 100, 2, 200);

		assertThat(c).isInstanceOf(Context2.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(2);
	}

	@Test
	public void of3() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300);

		assertThat(c).isInstanceOf(Context3.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(3);
	}

	@Test
	public void of4() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300, 4, 400);

		assertThat(c).isInstanceOf(Context4.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(4);
	}

	@Test
	public void of5() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300, 4, 400, 5, 500);

		assertThat(c).isInstanceOf(Context5.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(5);
	}

}
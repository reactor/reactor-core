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

import static org.assertj.core.api.Assertions.*;

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

	@Test
	public void of1NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 1))
		                                .withMessage("key");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null))
		                                .withMessage("value");
	}

	@Test
	public void of2NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 2, 2, 2))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(2, null, 2, 2))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(2, 2, null, 2))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(2, 2, 2, null))
		                                .withMessage("value2");
	}

	@Test
	public void of3NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 3, 3, 3, 3, 3))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(3, null, 3, 3, 3, 3))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(3, 3, null, 3, 3, 3))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(3, 3, 3, null, 3, 3))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(3, 3, 3, 3, null, 3))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(3, 3, 3, 3, 3, null))
		                                .withMessage("value3");
	}

	@Test
	public void of4NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 4, 4, 4, 4, 4, 4, 4))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(4, null, 4, 4, 4, 4, 4, 4))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(4, 4, null, 4, 4, 4, 4, 4))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(4, 4, 4, null, 4, 4, 4, 4))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(4, 4, 4, 4, null, 4, 4, 4))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(4, 4, 4, 4, 4, null, 4, 4))
		                                .withMessage("value3");
		assertThatNullPointerException().as("key4").isThrownBy(() -> Context.of(4, 4, 4, 4, 4, 4, null, 4))
		                                .withMessage("key4");
		assertThatNullPointerException().as("value4").isThrownBy(() -> Context.of(4, 4, 4, 4, 4, 4, 4, null))
		                                .withMessage("value4");
	}

	@Test
	public void of5NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 5, 5, 5, 5, 5, 5, 5, 5, 5))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(5, null, 5, 5, 5, 5, 5, 5, 5, 5))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(5, 5, null, 5, 5, 5, 5, 5, 5, 5))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(5, 5, 5, null, 5, 5, 5, 5, 5, 5))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(5, 5, 5, 5, null, 5, 5, 5, 5, 5))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(5, 5, 5, 5, 5, null, 5, 5, 5, 5))
		                                .withMessage("value3");
		assertThatNullPointerException().as("key4").isThrownBy(() -> Context.of(5, 5, 5, 5, 5, 5, null, 5, 5, 5))
		                                .withMessage("key4");
		assertThatNullPointerException().as("value4").isThrownBy(() -> Context.of(5, 5, 5, 5, 5, 5, 5, null, 5, 5))
		                                .withMessage("value4");
		assertThatNullPointerException().as("key5").isThrownBy(() -> Context.of(5, 5, 5, 5, 5, 5, 5, 5, null, 5))
		                                .withMessage("key5");
		assertThatNullPointerException().as("value5").isThrownBy(() -> Context.of(5, 5, 5, 5, 5, 5, 5, 5, 5, null))
		                                .withMessage("value5");
	}

	@Test
	public void checkDuplicateKeysZeroOne() {
		assertThatCode(Context::checkDuplicateKeys).as("zero").doesNotThrowAnyException();
		assertThatCode(() -> Context.checkDuplicateKeys("one")).as("one").doesNotThrowAnyException();
	}

	@Test
	public void checkDuplicateKeysTwo() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 1))
				.withMessage("Found duplicate key in Context.of() with 2 key-value pairs: 1");
	}

	@Test
	public void checkDuplicateKeysThree() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 1, 3))
				.withMessage("Found duplicate key in Context.of() with 3 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 1))
				.withMessage("Found duplicate key in Context.of() with 3 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 2))
				.withMessage("Found duplicate key in Context.of() with 3 key-value pairs: 2");
	}

	@Test
	public void checkDuplicateKeysFour() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 1, 3, 4))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 1, 4))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 1))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 2, 4))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 2");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 2))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 2");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 3))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 3");
	}

	@Test
	public void checkDuplicateKeysFive() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 1, 3, 4, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 1, 4, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 1, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 4, 1))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 2, 4, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 2");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 2, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 2");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 4, 2))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 2");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 3, 5))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 3");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 4, 3))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 3");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.checkDuplicateKeys(1, 2, 3, 4, 4))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 4");
	}

	@Test
	public void ofTwoRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 1, 0))
				.withMessage("Found duplicate key in Context.of() with 2 key-value pairs: 1");
	}

	@Test
	public void ofThreeRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 2, 0))
				.withMessage("Found duplicate key in Context.of() with 3 key-value pairs: 2");
	}

	@Test
	public void ofFourRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 3, 0))
				.withMessage("Found duplicate key in Context.of() with 4 key-value pairs: 3");
	}

	@Test
	public void ofFiveRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, 0, 4, 0))
				.withMessage("Found duplicate key in Context.of() with 5 key-value pairs: 4");
	}

}
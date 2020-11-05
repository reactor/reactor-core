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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

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
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 0))
		                                .withMessage("key");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null))
		                                .withMessage("value");
	}

	@Test
	public void of2NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 0, 2, 0))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null, 2, 0))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(1, 0, null, 0))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(1, 0, 2, null))
		                                .withMessage("value2");
	}

	@Test
	public void of3NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 0, 2, 0, 3, 0))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null, 2, 0, 3, 0))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(1, 0, null, 0, 3, 0))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(1, 0, 2, null, 3, 0))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(1, 0, 2, 0, null, 0))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, null))
		                                .withMessage("value3");
	}

	@Test
	public void of4NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 0, 2, 0, 3, 0, 4, 0))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null, 2, 0, 3, 0, 4, 0))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(1, 0, null, 0, 3, 0, 4, 0))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(1, 0, 2, null, 3, 0, 4, 0))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(1, 0, 2, 0, null, 0, 4, 0))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, null, 4, 0))
		                                .withMessage("value3");
		assertThatNullPointerException().as("key4").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, null, 0))
		                                .withMessage("key4");
		assertThatNullPointerException().as("value4").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, null))
		                                .withMessage("value4");
	}

	@Test
	public void of5NullChecks() {
		assertThatNullPointerException().as("key1").isThrownBy(() -> Context.of(null, 0, 2, 0, 3, 0, 4, 0, 5, 0))
		                                .withMessage("key1");
		assertThatNullPointerException().as("value1").isThrownBy(() -> Context.of(1, null, 2, 0, 3, 0, 4, 0, 5, 0))
		                                .withMessage("value1");
		assertThatNullPointerException().as("key2").isThrownBy(() -> Context.of(1, 0, null, 0, 3, 0, 4, 0, 5, 0))
		                                .withMessage("key2");
		assertThatNullPointerException().as("value2").isThrownBy(() -> Context.of(1, 0, 2, null, 3, 0, 4, 0, 5, 0))
		                                .withMessage("value2");
		assertThatNullPointerException().as("key3").isThrownBy(() -> Context.of(1, 0, 2, 0, null, 0, 4, 0, 5, 0))
		                                .withMessage("key3");
		assertThatNullPointerException().as("value3").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, null, 4, 0, 5, 0))
		                                .withMessage("value3");
		assertThatNullPointerException().as("key4").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, null, 0, 5, 0))
		                                .withMessage("key4");
		assertThatNullPointerException().as("value4").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, null, 5, 0))
		                                .withMessage("value4");
		assertThatNullPointerException().as("key5").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, 0, null, 0))
		                                .withMessage("key5");
		assertThatNullPointerException().as("value5").isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, 0, 5, null))
		                                .withMessage("value5");
	}

	@Test
	public void ofTwoRejectsDuplicates() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 1, 0))
				.withMessage("Key #1 (1) is duplicated");
	}

	@Test
	public void ofThreeRejectsDuplicates() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 1, 0, 3, 0))
				.withMessage("Key #1 (1) is duplicated");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 1, 0))
				.withMessage("Key #1 (1) is duplicated");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 2, 0))
				.withMessage("Key #2 (2) is duplicated");
	}

	//the other implementations rely on Context4.checkDuplicateKeys which is extensively tested in Context4Test
	@Test
	public void ofFourRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 3, 0))
				.withMessage("Key #3 (3) is duplicated");
	}

	@Test
	public void ofFiveRejectsSimpleDuplicate() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Context.of(1, 0, 2, 0, 3, 0, 4, 0, 4, 0))
				.withMessage("Key #4 (4) is duplicated");
	}

	@Test
	public void ofMapZero() {
		Map<String, Integer> map = new HashMap<>(0);

		assertThat(Context.of(map)).isInstanceOf(Context0.class);
	}

	@Test
	public void ofMapNull() {
		Map<String, Integer> nullMap = null;
		assertThatNullPointerException().isThrownBy(() -> Context.of(nullMap))
		                                .withMessage("map");
	}

	@Test
	public void ofMapOne() {
		Map<String, Integer> map = new HashMap<>(1);
		map.put("k1", 1);
		Context context = Context.of(map);

		assertThat(context).is(SIZE_1);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
	}

	@Test
	public void ofMapTwo() {
		Map<String, Integer> map = new HashMap<>(2);
		map.put("k1", 1);
		map.put("k2", 2);
		Context context = Context.of(map);

		assertThat(context).is(SIZE_2);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
		assertThat(context.getOrDefault("k2", 0)).isEqualTo(2);
	}

	@Test
	public void ofMapThree() {
		Map<String, Integer> map = new HashMap<>(3);
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		Context context = Context.of(map);

		assertThat(context).is(SIZE_3);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
		assertThat(context.getOrDefault("k2", 0)).isEqualTo(2);
		assertThat(context.getOrDefault("k3", 0)).isEqualTo(3);
	}

	@Test
	public void ofMapFour() {
		Map<String, Integer> map = new HashMap<>(4);
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		Context context = Context.of(map);

		assertThat(context).is(SIZE_4);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
		assertThat(context.getOrDefault("k2", 0)).isEqualTo(2);
		assertThat(context.getOrDefault("k3", 0)).isEqualTo(3);
		assertThat(context.getOrDefault("k4", 0)).isEqualTo(4);
	}

	@Test
	public void ofMapFive() {
		Map<String, Integer> map = new HashMap<>(5);
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		map.put("k5", 5);
		Context context = Context.of(map);

		assertThat(context).is(SIZE_5);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
		assertThat(context.getOrDefault("k2", 0)).isEqualTo(2);
		assertThat(context.getOrDefault("k3", 0)).isEqualTo(3);
		assertThat(context.getOrDefault("k4", 0)).isEqualTo(4);
		assertThat(context.getOrDefault("k5", 0)).isEqualTo(5);
	}

	@Test
	public void ofMapSix() {
		Map<String, Integer> map = new HashMap<>(6);
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		map.put("k5", 5);
		map.put("k6", 6);
		Context context = Context.of(map);

		assertThat(context).isInstanceOf(ContextN.class);
		assertThat(context.size()).as("size").isEqualTo(6);
		assertThat(context.getOrDefault("k1", 0)).isEqualTo(1);
		assertThat(context.getOrDefault("k2", 0)).isEqualTo(2);
		assertThat(context.getOrDefault("k3", 0)).isEqualTo(3);
		assertThat(context.getOrDefault("k4", 0)).isEqualTo(4);
		assertThat(context.getOrDefault("k5", 0)).isEqualTo(5);
		assertThat(context.getOrDefault("k6", 0)).isEqualTo(6);
	}

	@Test
	public void ofLinkedHashMap6() {
		//Context.of(map) should always avoid being backed directly by the given map since we don't know
		Map<String, Integer> map = new LinkedHashMap<>(6);
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		map.put("k5", 5);
		map.put("k6", 6);
		Context context = Context.of(map);

		assertThat(context).isInstanceOf(ContextN.class);
		assertThat(context.size()).as("size").isEqualTo(6);
	}

	@Test
	public void ofMapWithNullKey() {
		Map<Object, Object> map = new HashMap<>();
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		map.put("k5", 5);
		map.put(null, "foo");

		assertThatNullPointerException().isThrownBy(() -> Context.of(map))
		                                .withMessage("null key found");
	}

	@Test
	public void ofMapWithNullValue() {
		Map<Object, Object> map = new HashMap<>();
		map.put("k1", 1);
		map.put("k2", 2);
		map.put("k3", 3);
		map.put("k4", 4);
		map.put("k5", 5);
		map.put("k6", null);

		assertThatNullPointerException().isThrownBy(() -> Context.of(map))
		                                .withMessage("null value for key k6");
	}

	@Test
	public void ofContextViewNull() {
		assertThatNullPointerException().isThrownBy(() -> Context.of((ContextView) null))
		                                .withMessage("contextView");
	}

	@Test
	public void ofContextViewThatIsAContextReturnsSame() {
		ContextView cv = Context.of("A", "a", "B", "b");

		assertThat(Context.of(cv)).isSameAs(cv);
	}

	@Test
	public void ofContextViewThatIsntContextReturnsNewContext() {
		ForeignContextView cv = new ForeignContextView("A", "a");
		cv.directPut("B", "b");

		assertThat(Context.of(cv)).isNotSameAs(cv)
		                          .isExactlyInstanceOf(Context2.class);
	}

	// == tests for default methods ==

	@Test
	public void defaultPutAll() {
		Map<Object, Object> leftMap = new HashMap<>();
		leftMap.put(1, "A");
		leftMap.put(10, "A10");
		leftMap.put(11, "A11");
		leftMap.put(12, "A12");
		leftMap.put(13, "A13");
		Map<Object, Object> rightMap = new HashMap<>();
		rightMap.put(2, "B");
		rightMap.put(3, "C");
		rightMap.put(4, "D");
		rightMap.put(5, "E");
		ForeignContext left = new ForeignContext(leftMap);
		ForeignContext right = new ForeignContext(rightMap);

		Context combined = left.putAll(right.readOnly());
		assertThat(combined).isInstanceOf(ContextN.class);
		ContextN combinedN = (ContextN) combined;

		assertThat(combinedN)
				.containsKeys(1, 2, 3, 4, 5, 10, 11, 12, 13)
				.containsValues("A", "B", "C", "D", "E", "A10", "A11", "A12", "A13");
	}

	@Test
	public void defaultPutAllOtherIsAbstractContext() {
		Map<Object, Object> leftMap = new HashMap<>();
		leftMap.put(1, "A");
		leftMap.put(10, "A10");
		leftMap.put(11, "A11");
		leftMap.put(12, "A12");
		leftMap.put(13, "A13");
		Map<Object, Object> rightMap = new HashMap<>();
		rightMap.put(2, "B");
		rightMap.put(3, "C");
		rightMap.put(4, "D");
		rightMap.put(5, "E");
		ForeignContext left = new ForeignContext(leftMap);
		ContextN right = new ContextN(rightMap);
		right.accept(6, "F");

		Context combined = left.putAll(right.readOnly());
		assertThat(combined).isInstanceOf(ForeignContext.class);
		ForeignContext combinedN = (ForeignContext) combined;

		assertThat(combinedN.delegate)
				.containsKeys(1, 2, 3, 4, 5, 6, 10, 11, 12, 13)
				.containsValues("A", "B", "C", "D", "E", "F", "A10", "A11", "A12", "A13");
	}

	@Test
	public void defaultPutAllWorksWithParallelStream() {
		Map<Object, Object> leftMap = new HashMap<>();
		leftMap.put(1, "A");
		leftMap.put(10, "A10");
		leftMap.put(11, "A11");
		leftMap.put(12, "A12");
		leftMap.put(13, "A13");
		Map<Object, Object> rightMap = new HashMap<>();
		rightMap.put(2, "B");
		rightMap.put(3, "C");
		rightMap.put(4, "D");
		rightMap.put(5, "E");
		ForeignContext left = new ForeignContext(leftMap) {
			@Override
			public Stream<Map.Entry<Object, Object>> stream() {
				return super.stream().parallel();
			}
		};
		ForeignContext right = new ForeignContext(rightMap) {
			@Override
			public Stream<Map.Entry<Object, Object>> stream() {
				return super.stream().parallel();
			}
		};

		//this test proved flaky in the past, due to the parallelization
		for (int i = 0; i < 1000; i++) {
			Context combined = left.putAll(right.readOnly());
			assertThat(combined).isInstanceOf(ContextN.class);
			ContextN combinedN = (ContextN) combined;

			assertThat(combinedN)
					.containsKeys(1, 2, 3, 4, 5, 10, 11, 12, 13)
					.containsValues("A", "B", "C", "D", "E", "A10", "A11", "A12", "A13");
		}
	}

	@Test
	public void defaultPutAllForeignSmallSize() {
		Context initial = new ForeignContext(1, "A");
		Context other = new ForeignContext(2, "B");

		Context result = initial.putAll(other.readOnly());

		assertThat(result).isInstanceOf(Context2.class);
		Context2 context2 = (Context2) result;

		assertThat(context2.key1).as("key1").isEqualTo(1);
		assertThat(context2.value1).as("value1").isEqualTo("A");
		assertThat(context2.key2).as("key2").isEqualTo(2);
		assertThat(context2.value2).as("value2").isEqualTo("B");
	}

	@Test
	public void putAllForeignMiddleSize() {
		ForeignContext initial = new ForeignContext(1, "value1")
				.directPut(2, "value2")
				.directPut(3, "value3")
				.directPut(4, "value4");
		ForeignContext other = new ForeignContext(1, "replaced")
				.directPut(5, "value5")
				.directPut(6, "value6");

		Context result = initial.putAll(other.readOnly());

		assertThat(result).isInstanceOf(ContextN.class);
		ContextN resultN = (ContextN) result;
		assertThat(resultN)
				.containsKeys(1, 2, 3, 4, 5)
				.containsValues("replaced", "value2", "value3", "value4", "value5");
	}

	@Test
	public void putAllContextViewNoAmbiguity() {
		Context context = Context.of("key", "value");
		ContextView contextView = context;
		Context receiver = Context.of("foo", "bar");

		@SuppressWarnings("deprecation") // because of putAll(Context). This test method shall be removed in 3.5 alongside putAll(Context)
		Context resultFromContext = receiver.putAll(context);
		Context resultFromContextView = receiver.putAll(contextView);

		assertThat(resultFromContext.stream().collect(Collectors.toList()))
				.containsExactlyElementsOf(resultFromContextView.stream().collect(Collectors.toList()));
	}

	static class ForeignContext extends ForeignContextView implements Context {

		ForeignContext(Object key, Object value) {
			super(key, value);
		}

		ForeignContext(Map<Object, Object> data) {
			super(data);
		}

		@Override
		ForeignContext directPut(Object key, Object value) {
			super.directPut(key, value);
			return this;
		}

		@Override
		public Context put(Object key, Object value) {
			ForeignContext newContext = new ForeignContext(this.delegate);
			newContext.delegate.put(key, value);
			return newContext;
		}

		@Override
		public Context delete(Object key) {
			if (hasKey(key)) {
				ForeignContext newContext = new ForeignContext(this.delegate);
				newContext.delegate.remove(key);
				return newContext;
			}
			return this;
		}

		@Override
		public String toString() {
			return "ForeignContext" + delegate.toString();
		}
	}

	static class ForeignContextView implements ContextView {

		final Map<Object, Object> delegate = new LinkedHashMap<>();

		ForeignContextView(Object key, Object value) {
			this.delegate.put(key, value);
		}

		ForeignContextView(Map<Object, Object> data) {
			this.delegate.putAll(data);
		}

		ForeignContextView directPut(Object key, Object value) {
			this.delegate.put(key, value);
			return this;
		}

		@SuppressWarnings("unchecked")
		public <T> T get(Object key) {
			if (hasKey(key)) return (T) this.delegate.get(key);
			throw new NoSuchElementException();
		}

		public boolean hasKey(Object key) {
			return this.delegate.containsKey(key);
		}

		public int size() {
			return delegate.size();
		}

		public Stream<Map.Entry<Object, Object>> stream() {
			return delegate.entrySet().stream();
		}

		@Override
		public String toString() {
			return "ForeignContext" + delegate.toString();
		}
	}
}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.function;

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import java.lang.Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TupleTests {

	@Test
	public void tupleProvidesTypeSafeMethods() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);

		assertThat(t3.getT1()).as("first value is a string").isInstanceOf(String.class);
		assertThat(t3.getT2()).as("second value is a long").isInstanceOf(Long.class);
		assertThat(t3.getT3()).as("third value is an int").isInstanceOf(Integer.class);
	}

	@Test
	public void tupleProvidesTupleTypeHierarchy() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);

		assertThat(t3).as("Tuple3 is also a Tuple2").isInstanceOf(Tuple2.class);
	}

	@Test
	public void tupleEquals() {
		Tuple3<String, Long, Integer> t3a = Tuples.of("string", 1L, 10);
		Tuple3<String, Long, Integer> t3b = Tuples.of("string", 1L, 10);

		assertThat(t3a).as("Tuples of same length and values are equal.").isEqualTo(t3b);
	}

	@Test
	public void tupleNotEquals() {
		Tuple2<String, String> t2a = Tuples.of("ALPHA", "BRAVO");
		Tuple2<String, String> t2b = Tuples.of("ALPHA", "CHARLIE");

		assertThat(t2a).as("Tuples of same length and values are not equal.").isNotEqualTo(t2b);
	}

	@Test
	public void tuplesOfDifferentLengthAreNotEqual() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);
		Tuple2<String, Long> t2 = Tuples.of("string", 1L);

		assertThat(t3).as("Tuples of different length are not equal.").isNotEqualTo(t2);
		assertThat(t2).as("Tuples of different length are not equal.").isNotEqualTo(t3);
	}

	@Test
	public void fromArrayRejects0() {
		assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(new Object[0]))
		          .withMessageStartingWith("null or too small array, need between 2 and 8 values");
	}

	@Test
	public void fromArrayRejects1() {
		assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(new Object[1]))
		          .withMessageStartingWith("null or too small array, need between 2 and 8 values");
	}

	@Test
	public void fromArrayRejectsNull() {
		assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(null))
		          .withMessageStartingWith("null or too small array, need between 2 and 8 values");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple2CreatedFromArray2() {
		Integer[] source = new Integer[] { 1, 2 };
		Tuple2 expected = Tuples.of(1, 2);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple2.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple3CreatedFromArray3() {
		Integer[] source = new Integer[]{1, 2, 3};
		Tuple2 expected = Tuples.of(1, 2, 3);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual).isExactlyInstanceOf(Tuple3.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple4CreatedFromArray4() {
		Integer[] source = new Integer[] { 1, 2, 3, 4 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple4.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple5CreatedFromArray5() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple5.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple6CreatedFromArray6() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple6.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple7CreatedFromArray7() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6, 7);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple7.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple8CreatedFromArray8() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6, 7, 8);
		Tuple2 actual = Tuples.fromArray(source);

		assertThat(actual)
		          .isExactlyInstanceOf(Tuple8.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void fromArrayRejects9() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(source))
		          .withMessage("too many arguments (9), need between 2 and 8 values");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void fnAny() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple2<Object, Object> tuple = Tuples.fnAny().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fnAnyDelegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple2, Tuple2<Object, Object>> invert = t2 -> new Tuple2<>(t2.getT2(), t2.getT1());

		Tuple2<Object, Object> tuple = Tuples.fnAny(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(2);
		assertThat(tuple.getT2()).isEqualTo(1);
		assertThat(tuple)
				.isExactlyInstanceOf(Tuple2.class)
				.containsExactly(2, 1);
	}

	@Test
	public void fn2() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple2<Object, Object> tuple = Tuples.fn2().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn3() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple3<Object, Object, Object> tuple = Tuples.fn3().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn3Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple3<Integer, Integer, Integer>, Tuple3> invert = t3 -> new Tuple3<>(t3.getT3(), t3.getT2(), t3.getT1());

		Tuple3 tuple = Tuples.fn3(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(3);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(1);
		assertThat((Object) tuple).isExactlyInstanceOf(Tuple3.class);
	}

	@Test
	public void fn4() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple4<Object, Object, Object, Object> tuple = Tuples.fn4().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn4Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple4<Integer, Integer, Integer, Integer>, Tuple4> invert =
				t4 -> new Tuple4<>(t4.getT4(), t4.getT3(), t4.getT2(), t4.getT1());

		Tuple4 tuple = Tuples.fn4(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(4);
		assertThat(tuple.getT2()).isEqualTo(3);
		assertThat(tuple.getT3()).isEqualTo(2);
		assertThat(tuple.getT4()).isEqualTo(1);
		assertThat((Object) tuple).isExactlyInstanceOf(Tuple4.class);
	}

	@Test
	public void fn5() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple5<Object, Object, Object, Object, Object> tuple = Tuples.fn5().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple.getT5()).isEqualTo(5);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn5Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5> invert =
				t5 -> new Tuple5<>(t5.getT5(), t5.getT4(), t5.getT3(), t5.getT2(), t5.getT1());

		Tuple5 tuple = Tuples.fn5(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(5);
		assertThat(tuple.getT2()).isEqualTo(4);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(2);
		assertThat(tuple.getT5()).isEqualTo(1);
		assertThat((Object) tuple).isExactlyInstanceOf(Tuple5.class);
	}

	@Test
	public void fn6() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple6<Object, Object, Object, Object, Object, Object> tuple = Tuples.fn6().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple.getT5()).isEqualTo(5);
		assertThat(tuple.getT6()).isEqualTo(6);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn6Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6> invert =
				t6 -> new Tuple6<>(t6.getT6(), t6.getT5(), t6.getT4(), t6.getT3(), t6.getT2(), t6.getT1());

		Tuple6 tuple = Tuples.fn6(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(6);
		assertThat(tuple.getT2()).isEqualTo(5);
		assertThat(tuple.getT3()).isEqualTo(4);
		assertThat(tuple.getT4()).isEqualTo(3);
		assertThat(tuple.getT5()).isEqualTo(2);
		assertThat(tuple.getT6()).isEqualTo(1);
		assertThat((Object) tuple).isExactlyInstanceOf(Tuple6.class);
	}

	@Test
	public void fn7() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple7<Object, Object, Object, Object, Object, Object, Object> tuple = Tuples.fn7().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple.getT5()).isEqualTo(5);
		assertThat(tuple.getT6()).isEqualTo(6);
		assertThat(tuple.getT7()).isEqualTo(7);
		assertThat(tuple)
				.isInstanceOf(Tuple8.class)
				.containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
	}

	@Test
	public void fn7Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7> invert =
				t7 -> new Tuple7<>(t7.getT7(), t7.getT6(), t7.getT5(), t7.getT4(), t7.getT3(), t7.getT2(), t7.getT1());

		Tuple7 tuple = Tuples.fn7(invert).apply(source);

		assertThat(tuple.getT1()).isEqualTo(7);
		assertThat(tuple.getT2()).isEqualTo(6);
		assertThat(tuple.getT3()).isEqualTo(5);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple.getT5()).isEqualTo(3);
		assertThat(tuple.getT6()).isEqualTo(2);
		assertThat(tuple.getT7()).isEqualTo(1);
		assertThat((Object) tuple).isExactlyInstanceOf(Tuple7.class);
	}
	@Test
	public void fn8() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };

		Tuple8 tuple = Tuples.fn8().apply(source);

		assertThat(tuple.getT1()).isEqualTo(1);
		assertThat(tuple.getT2()).isEqualTo(2);
		assertThat(tuple.getT3()).isEqualTo(3);
		assertThat(tuple.getT4()).isEqualTo(4);
		assertThat(tuple.getT5()).isEqualTo(5);
		assertThat(tuple.getT6()).isEqualTo(6);
		assertThat(tuple.getT7()).isEqualTo(7);
		assertThat(tuple.getT8()).isEqualTo(8);
	}

	@Test
	public void fn8Delegate() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Function<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Integer> sum =
				t8 -> t8.getT8() + t8.getT7() + t8.getT6() + t8.getT5() + t8.getT4() + t8.getT3() + t8.getT2() + t8.getT1();

		Integer result = Tuples.fn8(sum).apply(source);

		assertThat(result).isEqualTo(36);
	}

	@Test
	public void tupleStringRepresentationFull() {
		assertThat(Tuples.tupleStringRepresentation(1, 2, 3, 4, 5)
		                 .toString())
				.isEqualTo("1,2,3,4,5");
	}

	@Test
	public void tupleStringRepresentationSparse() {
		assertThat(Tuples.tupleStringRepresentation(null, 2, null, 4, 5, null)
		                 .toString())
				.isEqualTo(",2,,4,5,");
	}

}

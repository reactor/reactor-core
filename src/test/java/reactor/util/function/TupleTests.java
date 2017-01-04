/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.function;

import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TupleTests {

	@Test
	public void tupleProvidesTypeSafeMethods() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);

		assertThat("first value is a string", String.class.isInstance(t3.getT1()));
		assertThat("second value is a long", Long.class.isInstance(t3.getT2()));
		assertThat("third value is an int", Integer.class.isInstance(t3.getT3()));
	}

	@Test
	public void tupleProvidesTupleTypeHierarchy() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);

		assertThat("Tuple3 is also a Tuple2", Tuple2.class.isInstance(t3));
	}

	@Test
	public void tupleEquals() {
		Tuple3<String, Long, Integer> t3a = Tuples.of("string", 1L, 10);
		Tuple3<String, Long, Integer> t3b = Tuples.of("string", 1L, 10);

		assertThat("Tuples of same length and values are equal.", t3a, is(t3b));
	}

	@Test
	public void tupleNotEquals() {
		Tuple2<String, String> t2a = Tuples.of("ALPHA", "BRAVO");
		Tuple2<String, String> t2b = Tuples.of("ALPHA", "CHARLIE");

		assertThat("Tuples of same length and values are not equal.", t2a, is(Matchers.not(t2b)));
	}

	@Test
	public void tuplesOfDifferentLengthAreNotEqual() {
		Tuple3<String, Long, Integer> t3 = Tuples.of("string", 1L, 10);
		Tuple2<String, Long> t2 = Tuples.of("string", 1L);

		assertThat("Tuples of different length are not equal.", t3, is(Matchers.not(t2)));
		assertThat("Tuples of different length are not equal.", t2, is(Matchers.not((Tuple2<String, Long>) t3)));
	}

	@Test
	public void fromArrayRejects0() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(new Object[0]))
		          .withMessageStartingWith("null or empty array, need between 1 and 8 values");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple2CreatedFromArray1() {
		Integer[] source = new Integer[] { 1 };
		Tuple2 expected = Tuples.of(1, null);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple2.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple2CreatedFromArray2() {
		Integer[] source = new Integer[] { 1, 2 };
		Tuple2 expected = Tuples.of(1, 2);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple2.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple3CreatedFromArray3() {
		Integer[] source = new Integer[]{1, 2, 3};
		Tuple2 expected = Tuples.of(1, 2, 3);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual).isExactlyInstanceOf(Tuple3.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple4CreatedFromArray4() {
		Integer[] source = new Integer[] { 1, 2, 3, 4 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple4.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple5CreatedFromArray5() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple5.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple6CreatedFromArray6() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple6.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple7CreatedFromArray7() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6, 7);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple7.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void tuple8CreatedFromArray8() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		Tuple2 expected = Tuples.of(1, 2, 3, 4, 5, 6, 7, 8);
		Tuple2 actual = Tuples.fromArray(source);

		Assertions.assertThat(actual)
		          .isExactlyInstanceOf(Tuple8.class)
		          .isEqualTo(expected);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void fromArrayRejects9() {
		Integer[] source = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> Tuples.fromArray(source))
		          .withMessage("too many arguments (9), need between 1 and 8 values");
	}

}

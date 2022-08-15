/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.function;

import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TupleMappersTest {

	@Test
	public void fn2() {
		BiFunction<Integer, Integer, Integer> function = Integer::sum;
		Tuple2<Integer, Integer> source = Tuples.of(1, 2);
		Integer expected = 3;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn3() {
		Function3<Integer, Integer, Integer, Integer> function = (one, two, three) -> one + two + three;
		Tuple3<Integer, Integer, Integer> source = Tuples.of(1, 2, 3);
		Integer expected = 6;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn4() {
		Function4<Integer, Integer, Integer, Integer, Integer> function =
				(one, two, three, four) -> one + two + three + four;
		Tuple4<Integer, Integer, Integer, Integer> source = Tuples.of(1, 2, 3, 4);
		Integer expected = 10;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn5() {
		Function5<Integer, Integer, Integer, Integer, Integer, Integer> function =
				(one, two, three, four, five) -> one + two + three + four + five;
		Tuple5<Integer, Integer, Integer, Integer, Integer> source = Tuples.of(1, 2, 3, 4, 5);
		Integer expected = 15;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn6() {
		Function6<Integer, Integer, Integer, Integer, Integer, Integer, Integer> function =
				(one, two, three, four, five, six) -> one + two + three + four + five + six;
		Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> source = Tuples.of(1, 2, 3, 4, 5, 6);
		Integer expected = 21;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn7() {
		Function7<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> function =
				(one, two, three, four, five, six, seven) -> one + two + three + four + five + six + seven;
		Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> source = Tuples.of(1, 2, 3, 4, 5, 6, 7);
		Integer expected = 28;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void fn8() {
		Function8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> function =
				(one, two, three, four, five, six, seven, eight) -> one + two + three + four + five + six + seven + eight;
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> source =
				Tuples.of(1, 2, 3, 4, 5, 6, 7, 8);
		Integer expected = 36;
		Integer actual = TupleMappers.fn(function).apply(source);
		assertThat(actual).isEqualTo(expected);
	}
}

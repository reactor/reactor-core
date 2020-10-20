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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class Tuple8Test {

	private Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> full =
			new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 8);

	@Test
	public void nullT8Rejected() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new Tuple8<>(1, 2, 3, 4, 5, 6, 7, null))
				.withMessage("t8");
	}

	@Test
	public void mapT1() {
		Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> base = 
				Tuples.of("Foo", 200, 300, 400, 500, 600, 700, 800);

		Tuple2<?,?> mapped = base.mapT1(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(3, base.getT2(), base.getT3(), base.getT4(),
				                  base.getT5(), base.getT6(), base.getT7(), base.getT8());
	}

	@Test
	public void mapT2() {
		Tuple8<Integer, String, Integer, Integer, Integer, Integer, Integer, Integer> base =
				Tuples.of(100, "Foo", 300, 400, 500, 600, 700, 800);

		Tuple2<?,?> mapped = base.mapT2(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), 3, base.getT3(), base.getT4(),
				                  base.getT5(), base.getT6(), base.getT7(), base.getT8());
	}

	@Test
	public void mapT3() {
		Tuple8<Integer, Integer, String, Integer, Integer, Integer, Integer, Integer> base = 
				Tuples.of(100, 200, "Foo", 400, 500, 600, 700, 800);

		Tuple2<?,?> mapped = base.mapT3(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), 3, base.getT4(),
				                  base.getT5(), base.getT6(), base.getT7(), base.getT8());
	}

	@Test
	public void mapT4() {
		Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, Integer> base =
				Tuples.of(100, 200, 300, "Foo", 500, 600, 700, 800);

		Tuple2<?,?> mapped = base.mapT4(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), base.getT3(), 3,
				                  base.getT5(), base.getT6(), base.getT7(), base.getT8());
	}

	@Test
	public void mapT5() {
		Tuple8<Integer, Integer, Integer, Integer, String, Integer, Integer, Integer> base = 
				Tuples.of(100, 200, 300, 400, "Foo", 600, 700, 800);

		Tuple2<?,?> mapped = base.mapT5(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), base.getT3(), base.getT4(),
				                  3, base.getT6(), base.getT7(), base.getT8());
	}

	@Test
	public void mapT6() {
		Tuple8<Integer, Integer, Integer, Integer, Integer, String, Integer, Integer> base = 
				Tuples.of(100, 200, 300, 400, 500, "Foo", 700, 800);

		Tuple2<?,?> mapped = base.mapT6(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), base.getT3(), base.getT4(),
				                  base.getT5(), 3, base.getT7(), base.getT8());
	}

	@Test
	public void mapT7() {
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, String, Integer> base =
				Tuples.of(100, 200, 300, 400, 500, 600, "Foo", 800);

		Tuple2<?,?> mapped = base.mapT7(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), base.getT3(), base.getT4(),
				                  base.getT5(), base.getT6(), 3, base.getT8());
	}

	@Test
	public void mapT8() {
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, String> base = 
				Tuples.of(100, 200, 300, 400, 500, 600, 700, "Foo");

		Tuple2<?,?> mapped = base.mapT8(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(8)
		                  .containsExactly(base.getT1(), base.getT2(), base.getT3(), base.getT4(),
				                  base.getT5(), base.getT6(), base.getT7(), 3);
	}

	@Test
	public void mapT8Null() {
		assertThatNullPointerException().isThrownBy(() ->
				Tuples.of(1, 2, 3, 4, 5, 6, 7, 8)
				      .mapT8(i -> null)
		).withMessage("t8");
	}

	@Test
	public void getNegativeIndex() {
		assertThat(full.get(-1)).isNull();
	}

	@Test
	public void getTooLargeIndex() {
		assertThat(full.get(10)).isNull();
	}

	@Test
	public void getAllValuesCorrespondToArray() {
		Object[] array = full.toArray();

		for (int i = 0; i < array.length; i++) {
			assertThat(full.get(i)).as("element at %d", i).isEqualTo(array[i]);
		}
	}

	@Test
	public void equalityOfSameReference() {
		assertThat(full).isEqualTo(full);
	}

	@Test
	public void equalityOfNullOrWrongClass() {
		assertThat(full).isNotEqualTo(null)
		                .isNotEqualTo("foo");
	}

	@Test
	public void t8Combinations() {
		assertThat(new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 8))
				.isNotEqualTo(new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 10))
				.isEqualTo(new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 8));
	}

	@Test
	public void sanityTestHashcode() {
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
				same = new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 8);
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
				different = new Tuple8<>(1, 2, 3, 4, 5, 6, 7, 1);

		assertThat(full.hashCode())
				.isEqualTo(same.hashCode())
				.isNotEqualTo(different.hashCode());
	}
}

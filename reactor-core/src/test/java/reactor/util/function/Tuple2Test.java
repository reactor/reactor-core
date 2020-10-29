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

public class Tuple2Test {

	private Tuple2<Integer, Integer> full = new Tuple2<>(1, 2);

	@Test
	public void nullT1Rejected() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new Tuple2<>(null, 2))
				.withMessage("t1");
	}

	@Test
	public void nullT2Rejected() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new Tuple2<>(1, null))
				.withMessage("t2");
	}
	@Test
	public void mapT1() {
		Tuple2<String, Integer> base = Tuples.of("Foo", 200);

		Tuple2<?,?> mapped = base.mapT1(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(2)
		                  .containsExactly(3, base.getT2());
	}

	@Test
	public void mapT2() {
		Tuple2<Integer, String> base = Tuples.of(100, "Foo");

		Tuple2<?,?> mapped = base.mapT2(String::length);

		assertThat(mapped).isNotSameAs(base)
		                  .hasSize(2)
		                  .containsExactly(base.getT1(), 3);
	}

	@Test
	public void mapT1Null() {
		assertThatNullPointerException().isThrownBy(() ->
				Tuples.of(1, 2)
				      .mapT1(i -> null)
		).withMessage("t1");
	}

	@Test
	public void mapT2Null() {
		assertThatNullPointerException().isThrownBy(() ->
				Tuples.of(1, 2)
				      .mapT2(i -> null)
		).withMessage("t2");
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
	public void equals() {
		Tuple2<Integer, Integer> otherFull = new Tuple2<>(1, 2);

		assertThat(full)
		        .isEqualTo(otherFull);
	}

	@Test
	public void invertedContentNotEquals() {
		Tuple2<Integer, Integer> otherFull = new Tuple2<>(2, 1);

		assertThat(full)
		        .isNotEqualTo(otherFull);
	}

	@Test
	public void sanityTestHashcode() {
		Tuple2<Integer, Integer> same = new Tuple2<>(1, 2);
		Tuple2<Integer, Integer> different = new Tuple2<>(2, 1);

		assertThat(full.hashCode())
				.isEqualTo(same.hashCode())
				.isNotEqualTo(different.hashCode());
	}

}

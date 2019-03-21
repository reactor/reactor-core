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

package reactor.util.function;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Tuple2Test {

	private Tuple2<Integer, Integer> empty = new Tuple2<>(null, null);
	private Tuple2<Integer, Integer> full = new Tuple2<>(1, 2);

	@Test
	public void sparseToString() {
		assertThat(new Tuple2<Integer, Integer>(null, 2))
				.hasToString("[,2]");

		assertThat(new Tuple2<Integer, Integer>(1, null))
				.hasToString("[1,]");
	}

	@Test
	public void nullsCountedInSize() {
		assertThat(empty.size()).isEqualTo(2);
		assertThat(empty).hasSize(2);
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
			assertThat(full.get(i)).as("element " + i).isEqualTo(array[i]);
		}
	}

	@Test
	public void sparseIsNotSameLeftAndRight() {
		Tuple2<Integer, Integer> sparseLeft = new Tuple2<>(null, 1);
		Tuple2<Integer, Integer> sparseRight = new Tuple2<>(1, null);

		assertThat(sparseLeft.hashCode())
				.isNotEqualTo(sparseRight.hashCode());

		assertThat(sparseLeft)
				.isNotEqualTo(sparseRight);
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
	public void equalsCombinations() {
		Tuple2<Integer, Integer> otherEmpty = new Tuple2<>(null, null);
		Tuple2<Integer, Integer> sparseLeft = new Tuple2<>(null, 2);
		Tuple2<Integer, Integer> sparseRight = new Tuple2<>(1, null);
		Tuple2<Integer, Integer> otherFull = new Tuple2<>(1, 2);

		assertThat(empty)
				.isEqualTo(otherEmpty)
				.isNotEqualTo(sparseLeft)
				.isNotEqualTo(sparseRight)
	            .isNotEqualTo(otherFull);

		assertThat(sparseLeft)
				.isNotEqualTo(otherEmpty)
				.isNotEqualTo(sparseRight)
		        .isNotEqualTo(otherFull);

		assertThat(sparseRight)
				.isNotEqualTo(otherEmpty)
				.isNotEqualTo(sparseLeft)
		        .isNotEqualTo(otherFull);

		assertThat(full)
				.isNotEqualTo(otherEmpty)
				.isNotEqualTo(sparseLeft)
				.isNotEqualTo(sparseRight)
		        .isEqualTo(otherFull);
	}

}
/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class Tuple7Test {

	private Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> full =
			new Tuple7<>(1, 2, 3, 4, 5, 6, 7);

	@Test
	public void nullT7Rejected() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new Tuple7<>(1, 2, 3, 4, 5, 6, null))
				.withMessage("t7");
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
	public void equalityOfSameReference() {
		assertThat(full).isEqualTo(full);
	}

	@Test
	public void equalityOfNullOrWrongClass() {
		assertThat(full).isNotEqualTo(null)
		                .isNotEqualTo("foo");
	}

	@Test
	public void t7Combinations() {
		assertThat(new Tuple7<>(1, 2, 3, 4, 5, 6, 7))
				.isNotEqualTo(new Tuple7<>(1, 2, 3, 4, 5, 6, 10))
				.isEqualTo(new Tuple7<>(1, 2, 3, 4, 5, 6, 7));
	}
}
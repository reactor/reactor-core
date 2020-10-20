/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

//https://github.com/reactor/reactor-core/pull/1326
public class QueuesOneQueueTest {

	private static final int TEST_ELEMENT = 2;

	@Test
	public void emptyOneQueueShouldConvertToArrayWhenPassedZeroLengthArray() {
		assertThat(emptyOneQueue().toArray(new Integer[0])).isEmpty();
	}

	@Test
	public void oneQueueWithOneElementShouldConvertToArrayWhenPassedZeroLengthArray() {
		assertThat(oneQueueWithTestElement(TEST_ELEMENT).toArray(new Integer[0]))
				.containsExactly(TEST_ELEMENT);
	}

	@Test
	public void emptyOneQueueShouldConvertToArrayAndPutNullMarkerAndReuseInputArrayOnWhenPassedOneLengthArray() {
		Queue<Integer> q = emptyOneQueue();
		//and
		Integer[] passedArray = new Integer[1];
		//when
		Integer[] convertedArray = q.toArray(passedArray);
		//then
		assertThat(convertedArray)
				.containsExactly((Integer)null)
				.isSameAs(passedArray);
	}

	@Test
	public void oneQueueWithOneElementShouldConvertToArrayAndReuseInputArrayWhenPassedOneLengthArray() {
		Queue<Integer> q = oneQueueWithTestElement(TEST_ELEMENT);
		//and
		Integer[] passedArray = new Integer[1];
		//when
		Integer[] convertedArray = q.toArray(passedArray);
		//then
		assertThat(convertedArray)
				.containsExactly(TEST_ELEMENT)
				.isSameAs(passedArray);
	}

	@Test
	public void emptyOneQueueShouldConvertToArrayAndPutNullMarkerAndReuseInputArrayWhenPassedLargerArray() {
		//given
		Queue<Integer> q = emptyOneQueue();
		//and
		Integer[] passedArray = {1, 2, 3};
		//when
		Integer[] convertedArray = q.toArray(passedArray);
		//then
		assertThat(convertedArray)
				.hasSize(3)
				.startsWith(null, 2, 3)
				.isSameAs(passedArray);
	}

	@Test
	public void oneQueueWithOneElementShouldConvertToArrayAndPutNullMarkerAndReuseInputArrayWhenPassedLargerArray() {
		Queue<Integer> q = oneQueueWithTestElement(TEST_ELEMENT);
		//and
		Integer[] passedArray = {1, 2, 3};
		//given
		Integer[] convertedArray = q.toArray(passedArray);
		//then
		assertThat(convertedArray)
				.hasSize(3)
				.startsWith(TEST_ELEMENT, null, 3)
				.isSameAs(passedArray);
	}

	private Queue<Integer> emptyOneQueue() {
		return Queues.<Integer>one().get();
	}

	private Queue<Integer> oneQueueWithTestElement(int element) {
		Queue<Integer> q = Queues.<Integer>one().get();
		q.add(element);
		return q;
	}
}

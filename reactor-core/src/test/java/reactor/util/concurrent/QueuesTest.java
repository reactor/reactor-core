/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.concurrent;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class QueuesTest {

	@Test
	public void capacityReactorUnboundedQueue() {
		Queue q = Queues.unbounded(2).get();

		assertThat(Queues.capacity(q)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void capacityReactorBoundedQueue() {
		//the bounded queue floors at 8 and rounds to the next power of 2

		assertThat(Queues.capacity(Queues.get(2).get()))
				.isEqualTo(8);

		assertThat(Queues.capacity(Queues.get(8).get()))
				.isEqualTo(8);

		assertThat(Queues.capacity(Queues.get(9).get()))
				.isEqualTo(16);
	}

	@Test
	public void capacityBoundedBlockingQueue() {
		Queue q = new LinkedBlockingQueue<>(10);

		assertThat(Queues.capacity(q)).isEqualTo(10);
	}

	@Test
	public void capacityUnboundedBlockingQueue() {
		Queue q = new LinkedBlockingQueue<>();

		assertThat(Queues.capacity(q)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void capacityUnboundedConcurrentLinkedQueue() {
		Queue q = new ConcurrentLinkedQueue<>();

		assertThat(Queues.capacity(q)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void capacityUnboundedMpscLinkedQueue() {
		Queue q = new MpscLinkedQueue();

		assertThat(Queues.capacity(q)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void capacityOtherQueue() {
		Queue q = new PriorityQueue<>(10);

		assertThat(Queues.capacity(q))
				.isEqualTo(Integer.MIN_VALUE)
				.isEqualTo(Queues.CAPACITY_UNSURE);
	}

	@Test
	public void zeroQueueOperations() {
		Queue<Integer> q = Queues.<Integer>empty().get();
		List<Integer> vals = Arrays.asList(1, 2, 3);

		assertThat(q.add(1)).as("add").isFalse();
		assertThat(q.addAll(vals)).as("addAll").isFalse();
		assertThat(q.offer(1)).as("offer").isFalse();

		assertThat(q.peek()).as("peek").isNull();
		assertThat(q.poll()).as("poll").isNull();
		assertThat(q.contains(1)).as("contains").isFalse();
		assertThat(q.iterator()).as("iterator").isEmpty();

		assertThatExceptionOfType(NoSuchElementException.class)
				.as("element")
				.isThrownBy(q::element);
		assertThatExceptionOfType(NoSuchElementException.class)
				.as("remove")
				.isThrownBy(q::remove);
		assertThat(q.remove(1)).as("remove").isFalse();

		assertThat(q.containsAll(vals)).as("containsAll").isFalse();
		assertThat(q.retainAll(vals)).as("retainAll").isFalse();
		assertThat(q.removeAll(vals)).as("removeAll").isFalse();

		assertThatCode(q::clear).as("clear").doesNotThrowAnyException();
		assertThat(q)
				.hasSize(0)
				.isEmpty();

		assertThat(q.toArray()).as("toArray").isEmpty();
		assertThat(q.toArray(new Integer[0])).as("toArray(empty)").isEmpty();

		Integer[] array = new Integer[] { -1, -2, -3 };
		assertThat(q.toArray(array)).as("toArray(pre-filled)").containsExactly(null, -2, -3);
	}

}
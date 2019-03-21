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

package reactor.util.concurrent;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

}
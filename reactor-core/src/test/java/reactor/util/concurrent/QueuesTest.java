/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.concurrent;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.core.publisher.Hooks;
import reactor.test.ParameterizedTestWithName;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
	public void capacityOneQueue() {
		Queue q = Queues.one().get();

		assertThat(Queues.capacity(q)).isEqualTo(1);
	}


	@Test
	public void capacityEmptyQueue() {
		Queue q = Queues.empty().get();

		assertThat(Queues.capacity(q)).isZero();
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
		assertThat(q.iterator()).as("iterator").isExhausted();

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

		Integer[] array = new Integer[]{-1, -2, -3};
		assertThat(q.toArray(array)).as("toArray(pre-filled)").containsExactly(null, -2, -3);
	}

	@Test    //https://github.com/reactor/reactor-core/issues/1326
	public void toArrayOnZeroQueueShouldNotFailAlsoOnJava9() {
		Queue<Integer> emptyQueue = Queues.<Integer>empty().get();

		assertThat(emptyQueue.toArray(new Integer[0])).as("toArray(empty)").isEmpty();
	}

	@ParameterizedTestWithName
	@MethodSource("queues")
	void testWrapping(Supplier<Queue<Object>> queue) {
		assertThat(queue.get()).as("no wrapper").hasSize(0);

		Hooks.addQueueWrapper("test", q -> {
			return new AbstractQueue<Object>() {

				@Override
				public int size() {
					return 42;
				}

				@Override
				public boolean offer(Object o) {
					throw new UnsupportedOperationException();
				}

				@Override
				public Object poll() {
					throw new UnsupportedOperationException();
				}

				@Override
				public Object peek() {
					throw new UnsupportedOperationException();
				}

				@Override
				public Iterator<Object> iterator() {
					throw new UnsupportedOperationException();
				}
			};
		});

		assertThat(queue.get()).as("with wrapper").hasSize(42);

		Hooks.removeQueueWrapper("test");

		assertThat(queue.get()).as("wrapper removed").hasSize(0);
	}

	private static Stream<Arguments> queues() {
		return Stream.of(
			arguments(named("one", Queues.one())),
			arguments(named("small", Queues.small())),
			arguments(named("xs", Queues.xs())),
			arguments(named("unbounded", Queues.unbounded())),
			arguments(named("unbounded(42)", Queues.unbounded(42))),
			arguments(named("unboundedMultiproducer", Queues.unboundedMultiproducer())),
			arguments(named("get(9000)", Queues.get(9000)))
		);
	}

}

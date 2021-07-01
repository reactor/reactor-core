/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SpscArrayQueueTest {

	@Test
	public void spscArrayQueuesAPI() {
		assertThat(Queues.xs().get()).isInstanceOf(SpscArrayQueue.class);
	}

	@Test
	public void shouldRejectNullableValues() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			q.offer(null);
		});
	}

	@Test
	public void shouldNotAllowIteratingWithIterator() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.iterator();
		});
	}

	@Test
	public void shouldNotAllowElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.remove(1);
		});
	}

	@Test
	public void shouldNotAllowAllElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.removeAll(Arrays.asList(1, 2));
		});
	}

	@Test
	public void shouldNotAllowAllElementsRetaining() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.retainAll(Arrays.asList(1, 2));
		});
	}

	@Test
	public void shouldNotAllowAdd() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.add(1);
		});
	}

	@Test
	public void shouldNotAllowAddAll() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.addAll(Arrays.asList(1, 2, 3));
		});
	}

	@Test
	public void shouldClearQueue() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(1);
		q.offer(2);

		assertThat(q.isEmpty()).as("isEmpty() false").isFalse();
		assertThat(q.size()).isEqualTo(2);

		q.clear();

		assertThat(q.isEmpty()).as("isEmpty() true").isTrue();
		assertThat(q.size()).isEqualTo(0);
	}

	@Test
	public void shouldNotRemoveElementOnPeek() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(1);
		q.offer(2);

		for (int i = 0; i < 100; i++) {
			assertThat(q.peek()).isEqualTo(1);
			assertThat(q.size()).isEqualTo(2);		}
	}
}

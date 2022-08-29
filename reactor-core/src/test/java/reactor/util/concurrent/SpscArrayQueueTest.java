/*
 * Copyright (c) 2018-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SpscArrayQueueTest {

	@Test
	void spscArrayQueuesAPI() {
		assertThat(Queues.xs().get()).isInstanceOf(SpscArrayQueue.class);
	}

	@Test
	void shouldRejectNullableValues() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			q.offer(null);
		});
	}

	@Test
	void shouldNotAllowIteratingWithIterator() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.iterator();
		});
	}

	@Test
	void shouldNotAllowElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.remove(1);
		});
	}

	@Test
	void shouldNotAllowAllElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.removeAll(Arrays.asList(1, 2));
		});
	}

	@Test
	void shouldNotAllowAllElementsRetaining() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.retainAll(Arrays.asList(1, 2));
		});
	}

	@Test
	void shouldNotAllowAdd() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.add(1);
		});
	}

	@Test
	void shouldNotAllowAddAll() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.addAll(Arrays.asList(1, 2, 3));
		});
	}

	@Test
	void shouldClearQueue() {
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
	void shouldNotRemoveElementOnPeek() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(1);
		q.offer(2);

		for (int i = 0; i < 100; i++) {
			assertThat(q.peek()).isEqualTo(1);
			assertThat(q.size()).isEqualTo(2);		}
	}

	@Test
	@Tag("slow")
	void objectPadding() {
		ClassLayout layout = ClassLayout.parseClass(SpscArrayQueue.class);

		AtomicLong currentPaddingSize = new AtomicLong();
		List<String> interestingFields = new ArrayList<>();
		List<Long> paddingSizes = new ArrayList<>();

		layout.fields().forEach(field -> {
			if (field.name().startsWith("pad")) {
				currentPaddingSize.addAndGet(field.size());
			}
			else {
				if (currentPaddingSize.get() > 0) {
					interestingFields.add("[padding]");
					paddingSizes.add(currentPaddingSize.getAndSet(0));
				}
				interestingFields.add(field.name());
			}
		});
		if (currentPaddingSize.get() > 0) {
			interestingFields.add("[padding]");
			paddingSizes.add(currentPaddingSize.getAndSet(0));
		}

		assertThat(interestingFields).containsExactly(
				"array",
				"mask",
				"[padding]",
				"producerIndex",
				"[padding]",
				"consumerIndex",
				"[padding]"
		);

		assertThat(paddingSizes).containsExactly(132L, 128L, 128L);
	}
}

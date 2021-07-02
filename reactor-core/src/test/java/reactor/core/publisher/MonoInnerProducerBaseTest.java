/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Operators.MonoInnerProducerBase;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoInnerProducerBaseTest {

	public static final int ITERATION_COUNT = 10_000;

	@Test
	public void completeWithValueCancelRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), Long.MAX_VALUE);
			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);

			Runnable r1 = () -> ds.complete(1);
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() == 1) {
				ts.assertValues(1);
			}
			else if (ts.values().size() > 1) {
				throw new AssertionError("Too many values received");
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}

	@Test
	public void requestCancelRaceAfterCompleteWithValue() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);
			ds.complete(1);

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}

	@Test
	public void requestCancelCompleteWithValueRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;
			Runnable r3 = () -> ds.complete(1);

			RaceTestUtils.race(r1, r2, r3);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}



	@Test
	public void completeWithoutValueCancelRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), Long.MAX_VALUE);
			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);
			ds.setValue(1);

			Runnable r1 = ds::complete;
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() == 1) {
				ts.assertValues(1);
			}
			else if (ts.values().size() > 1) {
				throw new AssertionError("Too many values received");
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}

	@Test
	public void requestCancelRaceAfterSetValueAndComplete() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);
			ds.setValue(1);
			ds.complete();

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}

	@Test
	public void requestCancelCompleteWithoutValueRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(Operators
					.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoInnerProducerBase<Integer> ds = new MonoInnerProducerBase<>(ts);
			ts.onSubscribe(ds);
			ds.setValue(1);

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;
			Runnable r3 = ds::complete;

			RaceTestUtils.race(r1, r2, r3);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
			else {
				assertThat(discarded.get()).isEqualTo(1);
			}
		}
	}
}

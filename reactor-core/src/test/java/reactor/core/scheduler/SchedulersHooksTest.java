/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import reactor.test.AutoDisposingRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class SchedulersHooksTest {

	@Rule
	public AutoDisposingRule afterTest = new AutoDisposingRule();

	@After
	public void resetAllHooks() {
		Schedulers.resetOnScheduleHooks();
	}

	@Test
	public void onScheduleIsAdditive() throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).as("3 decorators invoked").hasValue(111);
	}

	@Test
	public void onScheduleReplaces() throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(100);
	}

	@Test
	public void onScheduleWorksWhenEmpty() throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.resetOnScheduleHook("k1");

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@Test
	public void onScheduleIgnoresUnknownRemovals() {
		assertThatCode(() -> Schedulers.resetOnScheduleHook("k1"))
				.doesNotThrowAnyException();
	}

	@Test
	public void onScheduleResetOne() throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHook("k2");

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(101);
	}

	@Test
	public void onScheduleResetAll() throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHooks();

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@Test
	public void onSchedulesAreOrdered() throws Exception {
		CopyOnWriteArrayList<String> items = new CopyOnWriteArrayList<>();
		Schedulers.onScheduleHook("k1", new ApplicationOrderRecordingDecorator(items, "k1"));
		Schedulers.onScheduleHook("k2", new ApplicationOrderRecordingDecorator(items, "k2"));
		Schedulers.onScheduleHook("k3", new ApplicationOrderRecordingDecorator(items, "k3"));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "foo"))
		         .schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(items).containsExactly(
				"k1#0",
				"k2#0",
				"k3#0"
		);
	}

	private static class TrackingDecorator implements Function<Runnable, Runnable> {
		final AtomicInteger tracker;
		final int dx;

		private TrackingDecorator(AtomicInteger tracker, int dx) {
			this.tracker = tracker;
			this.dx = dx;
		}

		@Override
		public Runnable apply(Runnable runnable) {
			return () -> {
				tracker.addAndGet(dx);
				runnable.run();
			};
		}
	}

	private static class ApplicationOrderRecordingDecorator
			implements Function<Runnable, Runnable> {

		final List<String> items;

		final String id;

		final AtomicInteger counter = new AtomicInteger();

		public ApplicationOrderRecordingDecorator(List<String> items, String id) {
			this.items = items;
			this.id = id;
		}

		@Override
		public Runnable apply(Runnable runnable) {
			items.add(id + "#" + counter.getAndIncrement());
			return runnable;
		}
	}
}

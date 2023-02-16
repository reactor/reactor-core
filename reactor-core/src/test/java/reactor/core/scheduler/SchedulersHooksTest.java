/*
 * Copyright (c) 2019-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.AutoDisposingExtension;
import reactor.test.ParameterizedTestWithName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class SchedulersHooksTest {

	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@AfterEach
	public void resetAllHooks() {
		Schedulers.resetOnScheduleHooks();
		Schedulers.resetOnHandleError();
	}

	static Stream<Arguments> schedulers() {
		return Stream.of(
				arguments(Named.named("PARALLEL", (Supplier<Scheduler>) () -> Schedulers.newParallel("A", 1))),
				arguments(Named.named("BOUNDED_ELASTIC", (Supplier<Scheduler>) () -> Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "A"))),
				arguments(Named.named("SINGLE", (Supplier<Scheduler>) () -> Schedulers.newSingle("A"))),
				arguments(Named.named("EXECUTOR_SERVICE", (Supplier<Scheduler>) () -> Schedulers.fromExecutorService(Executors.newCachedThreadPool(), "A"))),
				arguments(Named.named("EXECUTOR", (Supplier<Scheduler>) () -> Schedulers.fromExecutor(task -> {
					Thread thread = new Thread(task);
					thread.setDaemon(true);
					thread.start();
				})))
		);
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	@Tag("slow")
	public void onScheduleIsAdditive(Supplier<Scheduler> schedulerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).as("3 decorators invoked").hasValue(111);
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	public void onScheduleReplaces(Supplier<Scheduler> schedulerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(100);
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	public void onScheduleWorksWhenEmpty(Supplier<Scheduler> schedulerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.resetOnScheduleHook("k1");

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@Test
	public void onScheduleIgnoresUnknownRemovals() {
		assertThatCode(() -> Schedulers.resetOnScheduleHook("k1"))
				.doesNotThrowAnyException();
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	@Tag("slow")
	public void onScheduleResetOne(Supplier<Scheduler> schedulerType) throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHook("k2");

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(101);
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	public void onScheduleResetAll(Supplier<Scheduler> schedulerType) throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHooks();

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("schedulers")
	public void onSchedulesAreOrdered(Supplier<Scheduler> schedulerType) throws Exception {
		CopyOnWriteArrayList<String> items = new CopyOnWriteArrayList<>();
		Schedulers.onScheduleHook("k1", new ApplicationOrderRecordingDecorator(items, "k1"));
		Schedulers.onScheduleHook("k2", new ApplicationOrderRecordingDecorator(items, "k2"));
		Schedulers.onScheduleHook("k3", new ApplicationOrderRecordingDecorator(items, "k3"));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(schedulerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(items).containsExactly(
				"k1#0",
				"k2#0",
				"k3#0"
		);
	}

	@Test
	void onHandleErrorWithKeyIsAdditive() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(1));
		Schedulers.onHandleError("k2", (t, error) -> tracker.addAndGet(10));
		Schedulers.onHandleError("k3", (t, error) -> tracker.addAndGet(100));

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).as("3 handlers invoked").hasValue(111);
	}

	@Test
	void onHandleErrorWithKeyReplaces() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(1));
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(10));
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(100));

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).as("last k1 invoked").hasValue(100);
	}

	@Test
	void onHandleErrorWithNoKeyIsAdditiveWithOtherKeys() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(1));
		Schedulers.onHandleError("k2", (t, error) -> tracker.addAndGet(10));
		Schedulers.onHandleError("k3", (t, error) -> tracker.addAndGet(100));

		Schedulers.handleError(new IllegalStateException("expected1"));

		assertThat(tracker).as("3 handlers invoked prior to setting anonymous hook part").hasValue(111);
		tracker.set(0);

		Schedulers.onHandleError((t, error) -> tracker.addAndGet(1000));
		Schedulers.handleError(new IllegalStateException("expected2"));

		assertThat(tracker).as("4 handlers invoked after anonymous hook part").hasValue(1111);
	}

	@Test
	void onHandleErrorWithKeyIgnoresUnknownRemovals() {
		assertThatCode(() -> Schedulers.resetOnHandleError("k1"))
			.doesNotThrowAnyException();
	}

	@Test
	void onHandleErrorResetOneKey() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(1));
		Schedulers.onHandleError("k2", (t, error) -> tracker.addAndGet(10));
		Schedulers.onHandleError("k3", (t, error) -> tracker.addAndGet(100));
		Schedulers.resetOnHandleError("k2");

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).hasValue(101);
	}

	@Test
	void onHandleErrorResetAll() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError("k1", (t, error) -> tracker.addAndGet(1));
		Schedulers.onHandleError("k2", (t, error) -> tracker.addAndGet(10));
		Schedulers.onHandleError("k3", (t, error) -> tracker.addAndGet(100));
		Schedulers.resetOnHandleError();

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).hasValue(0);
	}

	@Test
	void onHandleErrorResetAnonymousHook() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError((t, error) -> tracker.addAndGet(100));
		Schedulers.resetOnHandleError();

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).hasValue(0);
	}

	@Test
	void onHandleErrorWithUnknownSubKeyDoesntResetAnonymousHook() {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onHandleError((t, error) -> tracker.addAndGet(100));
		Schedulers.resetOnHandleError("k1");

		Schedulers.handleError(new IllegalStateException("expected"));

		assertThat(tracker).hasValue(100);
	}

	static class TrackingDecorator implements Function<Runnable, Runnable> {
		final AtomicInteger tracker;
		final int dx;

		TrackingDecorator(AtomicInteger tracker, int dx) {
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

	static class ApplicationOrderRecordingDecorator
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

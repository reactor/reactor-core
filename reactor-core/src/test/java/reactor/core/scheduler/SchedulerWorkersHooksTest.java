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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.scheduler.SchedulersHooksTest.ApplicationOrderRecordingDecorator;
import reactor.core.scheduler.SchedulersHooksTest.TrackingDecorator;
import reactor.test.AutoDisposingExtension;
import reactor.test.ParameterizedTestWithName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class SchedulerWorkersHooksTest {

	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@AfterEach
	public void resetAllHooks() {
		Schedulers.resetOnScheduleHooks();
		Schedulers.resetOnHandleError();
	}

	static Stream<Arguments> workers() {
		return Stream.of(
				arguments(Named.named("PARALLEL", (Supplier<Scheduler.Worker>)
						() -> Schedulers.newParallel("A", 1).createWorker())), 
				arguments(Named.named("BOUNDED_ELASTIC", (Supplier<Scheduler.Worker>)
						() -> Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "A").createWorker())),
				arguments(Named.named("SINGLE", (Supplier<Scheduler.Worker>)
						() -> Schedulers.newSingle("A").createWorker())),
				arguments(Named.named("EXECUTOR_SERVICE", (Supplier<Scheduler.Worker>)
						() -> Schedulers.fromExecutorService(Executors.newCachedThreadPool(), "A").createWorker())),
				arguments(Named.named("EXECUTOR", (Supplier<Scheduler.Worker>)
						() -> Schedulers.fromExecutor(task -> {
							Thread thread = new Thread(task);
							thread.setDaemon(true);
							thread.start();
						}).createWorker())));
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	@Tag("slow")
	public void onScheduleIsAdditive(Supplier<Scheduler.Worker> workerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).as("3 decorators invoked").hasValue(111);
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	public void onScheduleReplaces(Supplier<Scheduler.Worker> workerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 100));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(100);
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	public void onScheduleWorksWhenEmpty(Supplier<Scheduler.Worker> workerType) throws Exception {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.resetOnScheduleHook("k1");

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	@Tag("slow")
	public void onScheduleResetOne(Supplier<Scheduler.Worker> workerType) throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHook("k2");

		CountDownLatch latch = new CountDownLatch(3);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(101);
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	public void onScheduleResetAll(Supplier<Scheduler.Worker> workerType) throws InterruptedException {
		AtomicInteger tracker = new AtomicInteger();
		Schedulers.onScheduleHook("k1", new TrackingDecorator(tracker, 1));
		Schedulers.onScheduleHook("k2", new TrackingDecorator(tracker, 10));
		Schedulers.onScheduleHook("k3", new TrackingDecorator(tracker, 100));
		Schedulers.resetOnScheduleHooks();

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(tracker).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("workers")
	public void onSchedulesAreOrdered(Supplier<Scheduler.Worker> workerType) throws Exception {
		CopyOnWriteArrayList<String> items = new CopyOnWriteArrayList<>();
		Schedulers.onScheduleHook("k1", new ApplicationOrderRecordingDecorator(items, "k1"));
		Schedulers.onScheduleHook("k2", new ApplicationOrderRecordingDecorator(items, "k2"));
		Schedulers.onScheduleHook("k3", new ApplicationOrderRecordingDecorator(items, "k3"));

		CountDownLatch latch = new CountDownLatch(1);
		afterTest.autoDispose(workerType.get()).schedule(latch::countDown);
		latch.await(5, TimeUnit.SECONDS);

		assertThat(items).containsExactly(
				"k1#0",
				"k2#0",
				"k3#0"
		);
	}
}

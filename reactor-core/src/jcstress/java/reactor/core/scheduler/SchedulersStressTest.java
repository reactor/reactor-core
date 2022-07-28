/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZZ_Result;
import org.openjdk.jcstress.infra.results.Z_Result;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class SchedulersStressTest {

	private static void restart(Scheduler scheduler) {
		scheduler.disposeGracefully(Duration.ofMillis(100)).block(Duration.ofMillis(100));
		scheduler.start();
	}

	private static boolean canScheduleTask(Scheduler scheduler) {
		final CountDownLatch latch = new CountDownLatch(1);
		if (scheduler.isDisposed()) {
			return false;
		}
		scheduler.schedule(latch::countDown);
		boolean taskDone = false;
		try {
			taskDone = latch.await(100, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ignored) {
		}
		return taskDone;
	}

	@JCStressTest
	@Outcome(id = {"true"}, expect = Expect.ACCEPTABLE, desc = "Task scheduled after racing restart")
	@State
	public static class SingleSchedulerStartDisposeStressTest {

		final SingleScheduler scheduler = new SingleScheduler(Thread::new);
		{
			scheduler.start();
		}

		@Actor
		public void restart1() {
			restart(scheduler);
		}

		@Actor
		public void restart2() {
			restart(scheduler);
		}

		@Arbiter
		public void arbiter(Z_Result r) {
			// At this stage, at least one actor called scheduler.start(),
			// so we should be able to execute a task.
			r.r1 = canScheduleTask(scheduler);
			scheduler.dispose();
		}
	}

	@JCStressTest
	@Outcome(id = {"true"}, expect = Expect.ACCEPTABLE, desc = "Task scheduled after racing restart")
	@State
	public static class ParallelSchedulerStartDisposeStressTest {

		final ParallelScheduler scheduler = new ParallelScheduler(4, Thread::new);
		{
			scheduler.start();
		}

		@Actor
		public void restart1() {
			restart(scheduler);
		}

		@Actor
		public void restart2() {
			restart(scheduler);
		}

		@Arbiter
		public void arbiter(Z_Result r) {
			// At this stage, at least one actor called scheduler.start(),
			// so we should be able to execute a task.
			r.r1 = canScheduleTask(scheduler);
			scheduler.dispose();
		}
	}

	@JCStressTest
	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE,
			desc = "Both time out, task gets rejected, scheduler disposed eventually")
	@State
	public static class SingleSchedulerDisposeGracefullyStressTest extends RacingDisposeGracefullyStressTest<SingleScheduler> {

		@Override
		SingleScheduler createScheduler() {
			return new SingleScheduler(Thread::new);
		}

		@Override
		boolean isTerminated() {
			return scheduler.state.currentResource.isTerminated();
		}

		{
			scheduler.start();
			// Schedule a task that disallows graceful closure until the arbiter kicks in
			// to make sure that actors fail while waiting.
			scheduler.schedule(this::awaitArbiter);
		}

		@Actor
		public void disposeGracefully1(ZZZ_Result r) {
			r.r1 = checkDisposeGracefullyTimesOut();
		}

		@Actor
		public void disposeGracefully2(ZZZ_Result r) {
			r.r2 = checkDisposeGracefullyTimesOut();
		}

		@Arbiter
		public void arbiter(ZZZ_Result r) {
			// Release the task blocking graceful closure.
			arbiterStarted();
			r.r3 = validateSchedulerDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE,
			desc = "Both time out, task gets rejected, scheduler disposed eventually")
	@State
	public static class ParallelSchedulerDisposeGracefullyStressTest extends RacingDisposeGracefullyStressTest<ParallelScheduler> {

		@Override
		ParallelScheduler createScheduler() {
			return new ParallelScheduler(10, Thread::new);
		}

		@Override
		boolean isTerminated() {
			assert scheduler.state.initialResource != null;
			for (ScheduledExecutorService executor : scheduler.state.initialResource) {
				if (!executor.isTerminated()) {
					return false;
				}
			}
			return true;
		}

		{
			scheduler.start();
			// Schedule a task that disallows graceful closure until the arbiter kicks in
			// to make sure that actors fail while waiting.
			scheduler.schedule(this::awaitArbiter);
		}

		@Actor
		public void disposeGracefully1(ZZZ_Result r) {
			r.r1 = checkDisposeGracefullyTimesOut();
		}

		@Actor
		public void disposeGracefully2(ZZZ_Result r) {
			r.r2 = checkDisposeGracefullyTimesOut();
		}

		@Arbiter
		public void arbiter(ZZZ_Result r) {
			// Release the task blocking graceful closure.
			arbiterStarted();
			r.r3 = validateSchedulerDisposed();
		}
	}

	// The BoundedElasticScheduler stress tests never stop due to JCStress not surfacing OOM errors in spawned JVMs.
	// Perhaps tuning the memory consumption, combined with allowing the jcstress gradle plugin to pass heapPerFork
	// argument to JCStress runtime will make it feasible to run them.
	// For now, these tests reside in BoundedElasticSchedulerTest as
	// schedulerDisposeGracefullyConcurrentBothTimeout and schedulerStartedAfterConcurrentRestart.

//	@JCStressTest
//	@Outcome(id = {"true"}, expect = Expect.ACCEPTABLE, desc = "Task scheduled after racing restart")
//	@State
//	public static class BoundedElasticSchedulerStartDisposeStressTest {
//
//		final BoundedElasticScheduler scheduler = new BoundedElasticScheduler(1, 1, Thread::new, 5);
//		{
//			scheduler.start();
//		}
//
//		@Actor
//		public void restart1() {
//			restart(scheduler);
//		}
//
//		@Actor
//		public void restart2() {
//			restart(scheduler);
//		}
//
//		@Arbiter
//		public void arbiter(Z_Result r) {
//			// At this stage, at least one actor called scheduler.start(),
//			// so we should be able to execute a task.
//			r.r1 = canScheduleTask(scheduler);
//			scheduler.dispose();
//		}
//	}

//	@JCStressTest
//	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE,
//			desc = "Both time out, task gets rejected, scheduler disposed eventually")
//	@State
//	public static class BoundedElasticSchedulerDisposeGracefullyStressTest
//			extends RacingDisposeGracefullyStressTest<BoundedElasticScheduler> {
//
//		@Override
//		BoundedElasticScheduler createScheduler() {
//			return new BoundedElasticScheduler(4, 4, Thread::new, 5);
//		}
//
//		@Override
//		boolean isTerminated() {
//			for (BoundedElasticScheduler.BoundedState bs  : scheduler.state.boundedServices.busyArray) {
//				if (!bs.executor.isTerminated()) {
//					return false;
//				}
//			}
//			for (BoundedElasticScheduler.BoundedState bs  : scheduler.state.boundedServicesBeforeShutdown.busyArray) {
//				if (!bs.executor.isTerminated()) {
//					return false;
//				}
//			}
//			return scheduler.state.boundedServices.idleQueue.isEmpty();
//		}
//
//		{
//			scheduler.start();
//			// Schedule a task that disallows graceful closure until the arbiter kicks in
//			// to make sure that actors fail while waiting.
//			scheduler.schedule(this::awaitArbiter);
//		}
//
//		@Actor
//		public void disposeGracefully1(ZZZ_Result r) {
//			r.r1 = checkDisposeGracefullyTimesOut();
//		}
//
//		@Actor
//		public void disposeGracefully2(ZZZ_Result r) {
//			r.r2 = checkDisposeGracefullyTimesOut();
//		}
//
//		@Arbiter
//		public void arbiter(ZZZ_Result r) {
//			// Release the task blocking graceful closure.
//			arbiterStarted();
//			r.r3 = validateSchedulerDisposed();
//		}
//	}
}

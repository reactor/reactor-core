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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIZ_Result;
import org.openjdk.jcstress.infra.results.Z_Result;

public abstract class SchedulersStressTest {

	private static void restart(Scheduler scheduler) {
		scheduler.disposeGracefully().block(Duration.ofMillis(100));
		// TODO: in 3.6.x: remove restart capability and this validation
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

		private final SingleScheduler scheduler = new SingleScheduler(Thread::new);

		{
			scheduler.init();
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

		private final ParallelScheduler scheduler =
				new ParallelScheduler(4, Thread::new);

		{
			scheduler.init();
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
	public static class BoundedElasticSchedulerStartDisposeStressTest {

		private final BoundedElasticScheduler scheduler =
				new BoundedElasticScheduler(1, 1, Thread::new, 5);
		{
			scheduler.init();
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
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"eventually disposed.")
	@State
	public static class SingleSchedulerDisposeGracefullyStressTest {

		private final SingleScheduler scheduler = new SingleScheduler(Thread::new);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully1(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void disposeGracefully2(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"eventually disposed.")
	@State
	public static class ParallelSchedulerDisposeGracefullyStressTest {

		private final ParallelScheduler scheduler =
				new ParallelScheduler(10, Thread::new);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully1(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void disposeGracefully2(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"eventually disposed.")
	@State
	public static class BoundedElasticSchedulerDisposeGracefullyStressTest {

		private final BoundedElasticScheduler scheduler =
				new BoundedElasticScheduler(4, 4, Thread::new, 5);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully1(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void disposeGracefully2(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"disposeGracefully, eventually disposed.")
	@State
	public static class SingleSchedulerDisposeGracefullyAndDisposeStressTest {

		private final SingleScheduler scheduler = new SingleScheduler(Thread::new);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void dispose(IIZ_Result r) {
			scheduler.dispose();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"disposeGracefully, eventually disposed.")
	@State
	public static class ParallelSchedulerDisposeGracefullyAndDisposeStressTest {

		private final ParallelScheduler scheduler =
				new ParallelScheduler(10, Thread::new);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void dispose(IIZ_Result r) {
			scheduler.dispose();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {".*, true"}, expect = Expect.ACCEPTABLE,
			desc = "Scheduler in consistent state upon concurrent dispose and " +
					"disposeGracefully, eventually disposed.")
	@State
	public static class BoundedElasticSchedulerDisposeGracefullyAndDisposeStressTest {


		private final BoundedElasticScheduler scheduler =
				new BoundedElasticScheduler(4, 4, Thread::new, 5);

		{
			scheduler.init();
		}

		@Actor
		public void disposeGracefully(IIZ_Result r) {
			scheduler.disposeGracefully().subscribe();
			r.r1 = scheduler.state.initialResource.hashCode();
		}

		@Actor
		public void dispose(IIZ_Result r) {
			scheduler.dispose();
			r.r2 = scheduler.state.initialResource.hashCode();
		}

		@Arbiter
		public void arbiter(IIZ_Result r) {
			// Validate both disposals left the Scheduler in consistent state,
			// assuming the await process coordinates on the resources as identified
			// by r.r1 and r.r2, which should be equal.
			boolean consistentState = r.r1 == r.r2;
			r.r3 = consistentState && scheduler.isDisposed();
		}
	}
}

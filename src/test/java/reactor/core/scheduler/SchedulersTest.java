/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.concurrent.ThreadFactory;

import org.junit.After;
import org.junit.Test;
import org.testng.Assert;

public class SchedulersTest {

	final static class TestSchedulers implements Schedulers.Factory {

		final Scheduler      elastic  =
				Schedulers.Factory.super.newElastic(60, Thread::new);
		final Scheduler      single   = Schedulers.Factory.super.newSingle(Thread::new);
		final Scheduler      parallel =
				Schedulers.Factory.super.newParallel(1, Thread::new);
		final TimedScheduler timer    = Schedulers.Factory.super.newTimer(Thread::new);

		public TestSchedulers(boolean shutdownOnInit) {
			if (shutdownOnInit) {
				elastic.shutdown();
				single.shutdown();
				parallel.shutdown();
				timer.shutdown();
			}
		}

		public final Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
			return elastic;
		}

		public final Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
			return parallel;
		}

		public final Scheduler newSingle(ThreadFactory threadFactory) {
			return single;
		}

		public final TimedScheduler newTimer(ThreadFactory threadFactory) {
			return timer;
		}
	}

	@After
	public void resetSchedulers() {
		Schedulers.resetFactory();
	}

	@Test
	public void testOverride() throws InterruptedException {

		TestSchedulers ts = new TestSchedulers(true);
		Schedulers.setFactory(ts);

		Assert.assertEquals(ts.single, Schedulers.newSingle("unused"));
		Assert.assertEquals(ts.elastic, Schedulers.newElastic("unused"));
		Assert.assertEquals(ts.parallel, Schedulers.newParallel("unused"));
		Assert.assertEquals(ts.timer, Schedulers.newTimer("unused"));

		Schedulers.resetFactory();

		Scheduler s = Schedulers.newSingle("unused");
		s.shutdown();

		Assert.assertNotEquals(ts.single, s);
	}

	@Test
	public void testShutdownOldOnSetFactory() {
		Schedulers.Factory ts1 = new Schedulers.Factory() { };
		Schedulers.Factory ts2 = new TestSchedulers(false);
		Schedulers.setFactory(ts1);
		TimedScheduler cachedTimerOld = ((Schedulers.CachedTimedScheduler) Schedulers.timer()).cachedTimed;
		TimedScheduler standaloneTimer = Schedulers.newTimer("standaloneTimer");

		Assert.assertNotEquals(cachedTimerOld, standaloneTimer);
		Assert.assertNotEquals(cachedTimerOld.schedule(() -> {}), Scheduler.REJECTED);
		Assert.assertNotEquals(standaloneTimer.schedule(() -> {}), Scheduler.REJECTED);

		Schedulers.setFactory(ts2);
		TimedScheduler cachedTimerNew = ((Schedulers.CachedTimedScheduler) Schedulers.timer()).cachedTimed;

		Assert.assertEquals(cachedTimerNew, Schedulers.newTimer("unused"));
		Assert.assertNotEquals(cachedTimerNew, cachedTimerOld);
		//assert that the old factory's cached scheduler was shut down
		Assert.assertEquals(cachedTimerOld.schedule(() -> {}), Scheduler.REJECTED);
		//independently created schedulers are still the programmer's responsibility
		Assert.assertNotEquals(standaloneTimer.schedule(() -> {}), Scheduler.REJECTED);
		//new factory = new alive cached scheduler
		Assert.assertNotEquals(cachedTimerNew.schedule(() -> {}), Scheduler.REJECTED);
	}

}

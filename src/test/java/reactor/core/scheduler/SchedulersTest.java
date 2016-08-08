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

		public TestSchedulers() {
			elastic.shutdown();
			single.shutdown();
			parallel.shutdown();
			timer.shutdown();
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

	@Test
	public void testOverride() throws InterruptedException {

		TestSchedulers ts = new TestSchedulers();
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

}

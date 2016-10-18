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

package reactor.test.scheduler;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

/**
 * @author Stephane Maldini
 */
public class TestSchedulerTests {

	@Test
	public void allEnabled() {
		Assert.assertFalse(Schedulers.newTimer("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newParallel("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newElastic("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newSingle("") instanceof TestScheduler);

		TestScheduler.enable(true);

		Assert.assertTrue(Schedulers.newParallel("") instanceof TestScheduler);
		Assert.assertTrue(Schedulers.newElastic("") instanceof TestScheduler);
		Assert.assertTrue(Schedulers.newSingle("") instanceof TestScheduler);
		Assert.assertTrue(Schedulers.newTimer("") instanceof TestScheduler);

		TestScheduler t = TestScheduler.get();

		Assert.assertEquals(Schedulers.newParallel(""), t);
		Assert.assertEquals(Schedulers.newElastic(""), t);
		Assert.assertEquals(Schedulers.newSingle(""), t);
		Assert.assertEquals(Schedulers.newTimer(""), t);
	}

	@Test
	public void timerOnlyEnabled() {
		Assert.assertFalse(Schedulers.newTimer("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newParallel("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newElastic("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newSingle("") instanceof TestScheduler);

		TestScheduler.enable(false);

		Assert.assertTrue(Schedulers.newTimer("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newParallel("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newElastic("") instanceof TestScheduler);
		Assert.assertFalse(Schedulers.newSingle("") instanceof TestScheduler);

		TestScheduler t = TestScheduler.get();

		Assert.assertNotEquals(Schedulers.newParallel(""), t);
		Assert.assertNotEquals(Schedulers.newElastic(""), t);
		Assert.assertNotEquals(Schedulers.newSingle(""), t);
		Assert.assertEquals(Schedulers.newTimer(""), t);
	}

	@After
	public void cleanup() {
		TestScheduler.reset();
	}

}